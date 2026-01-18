# scripts/step4_stream_imbalance_with_filters.py
import pandas as pd
import numpy as np

# config
RATIO_UP_FACTOR = 3.0
RATIO_DOWN_FACTOR = 1/3.0
SCORE_SPIKE = 40
SCORE_RATIO = 40
SCORE_DISTRICT = 20
EPS = 1e-9
RATIO_CAP = 1e6  # for humanized output

# load
monthly = pd.read_csv("data/processed/monthly_pin_level.csv")
baseline = pd.read_csv("data/processed/pincode_baseline_stats.csv")

# merge baseline
df = monthly.merge(baseline, on="pincode", how="left", suffixes=("", "_base"))

# compute current ratios
df['current_bio_demo_ratio'] = df['bio_updates'] / (df['demo_updates'] + EPS)
df['current_enroll_demo_ratio'] = df['enrollments'] / (df['demo_updates'] + EPS)

# baseline safe ratio
df['bio_demo_ratio'] = df['bio_demo_ratio'].fillna(EPS)
df['enroll_demo_ratio'] = df['enroll_demo_ratio'].fillna(EPS)

# ratio changes (raw)
df['bio_demo_ratio_change'] = df['current_bio_demo_ratio'] / (df['bio_demo_ratio'] + EPS)
df['enroll_demo_ratio_change'] = df['current_enroll_demo_ratio'] / (df['enroll_demo_ratio'] + EPS)

# spike detection (reuse step3 logic if exists)
# prepare q3/iqr: ensure baseline columns present (fallback if missing)
if 'q3_monthly_total' not in baseline.columns or 'iqr_monthly_total' not in baseline.columns:
    raise RuntimeError("Baseline missing q3_monthly_total or iqr_monthly_total - rerun step2")

df['spike_threshold'] = df['q3_monthly_total'] + 3 * df['iqr_monthly_total']
df['spike_flag'] = df['total_monthly_count'] > df['spike_threshold']
df.loc[df['iqr_ratio'] > 2.5, 'spike_flag'] = False  # volatility guardrail

# district dominance (only if district exists)
if 'district' in df.columns:
    district_totals = df.groupby(['district','month'], as_index=False)['total_monthly_count'].sum().rename(columns={'total_monthly_count':'district_month_total'})
    df = df.merge(district_totals, on=['district','month'], how='left')
    df['pincode_share_of_district'] = df['total_monthly_count'] / (df['district_month_total'] + EPS)
    df['base_mean_share'] = df.groupby('pincode')['pincode_share_of_district'].transform('mean')
    df['base_share_std'] = df.groupby('pincode')['pincode_share_of_district'].transform('std').fillna(0)
    df['district_dominance_anom'] = (df['pincode_share_of_district'] > (df['base_mean_share'] + 3 * df['base_share_std']))
else:
    df['pincode_share_of_district'] = np.nan
    df['base_mean_share'] = np.nan
    df['base_share_std'] = np.nan
    df['district_dominance_anom'] = False

# initial scoring (simple)
df['anom_score'] = 0
df.loc[df['spike_flag'], 'anom_score'] += SCORE_SPIKE
df.loc[(df['bio_demo_ratio_change'] > RATIO_UP_FACTOR) | (df['bio_demo_ratio_change'] < RATIO_DOWN_FACTOR), 'anom_score'] += SCORE_RATIO
df.loc[(df['enroll_demo_ratio_change'] > RATIO_UP_FACTOR) | (df['enroll_demo_ratio_change'] < RATIO_DOWN_FACTOR), 'anom_score'] += int(SCORE_RATIO/2)
df.loc[df['district_dominance_anom'], 'anom_score'] += SCORE_DISTRICT
df['anom_score'] = df['anom_score'].clip(upper=100)

# Save raw flagged rows (where any of the basic flags triggered)
raw_flags = df[
    (df['spike_flag']) |
    ( (df['bio_demo_ratio_change'] > RATIO_UP_FACTOR) | (df['bio_demo_ratio_change'] < RATIO_DOWN_FACTOR) ) |
    ( (df['enroll_demo_ratio_change'] > RATIO_UP_FACTOR) | (df['enroll_demo_ratio_change'] < RATIO_DOWN_FACTOR) ) |
    (df['district_dominance_anom'])
].copy()

raw_flags.to_csv("data/processed/monthly_stream_imbalances.csv", index=False)

# first precision filter (same as before) -> high_confidence
# e.g. keep rows with score>=40 or any ratio >=5x or spike+ratio
high_cond = (
    (raw_flags['anom_score'] >= 40) |
    ( (raw_flags['bio_demo_ratio_change'] >= 5) | (raw_flags['enroll_demo_ratio_change'] >= 5) ) |
    ( raw_flags['spike_flag'] & ( (raw_flags['bio_demo_ratio_change']>=3) | (raw_flags['enroll_demo_ratio_change']>=3) ) )
)
high_conf = raw_flags[high_cond].copy()
high_conf.to_csv("data/processed/high_confidence_anomalies.csv", index=False)

# final strong filter (require >=2 strong conditions)
cond_score = high_conf['anom_score'] >= 70
cond_spike_plus_ratio = high_conf['spike_flag'] & ( (high_conf['bio_demo_ratio_change']>=3) | (high_conf['enroll_demo_ratio_change']>=3) )
cond_big_ratio = (high_conf['bio_demo_ratio_change'] >= 6) | (high_conf['enroll_demo_ratio_change'] >= 6)
cond_big_volume = high_conf['total_monthly_count'] >= 2 * high_conf['median_monthly_total'].fillna(0)

high_conf['strong_count'] = (
    cond_score.astype(int) +
    cond_spike_plus_ratio.astype(int) +
    cond_big_ratio.astype(int) +
    cond_big_volume.astype(int)
)

final = high_conf[high_conf['strong_count'] >= 2].copy()

# humanize big ratio numbers for nice CSV
def cap_ratio_val(v, cap=RATIO_CAP):
    try:
        if pd.isna(v): return v
        if np.isinf(v): return f">{int(cap)}x"
        if v >= cap: return f">{int(cap)}x"
        return round(v, 1)
    except Exception:
        return v

final['bio_demo_ratio_change_human'] = final['bio_demo_ratio_change'].apply(lambda v: cap_ratio_val(v))
final['enroll_demo_ratio_change_human'] = final['enroll_demo_ratio_change'].apply(lambda v: cap_ratio_val(v))

final.to_csv("data/processed/final_anomalies.csv", index=False)
final.to_csv("data/processed/final_anomalies_humanized.csv", index=False)

# finished
print("Wrote:")
print(" - data/processed/monthly_stream_imbalances.csv  (raw flags)")
print(" - data/processed/high_confidence_anomalies.csv (first filter)")
print(" - data/processed/final_anomalies.csv           (final, high precision)")
print(" - data/processed/final_anomalies_humanized.csv (final with capped ratio values)")
print("Counts -> raw:", len(raw_flags), "high_conf:", len(high_conf), "final:", len(final))
