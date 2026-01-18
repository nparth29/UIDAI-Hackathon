import pandas as pd

# -------------------------------------------------
# STEP 2: Build per-pincode baseline statistics
# -------------------------------------------------

# ---------- 1. Load processed monthly data ----------
df = pd.read_csv("data/processed/monthly_pin_level.csv")

# ---------- 2. Hard sanity checks ----------
assert df.isna().sum().sum() == 0, "NaN values found in monthly data"
required_cols = [
    'pincode',
    'month',
    'bio_updates',
    'demo_updates',
    'enrollments',
    'total_monthly_count'
]
missing = [c for c in required_cols if c not in df.columns]
assert len(missing) == 0, f"Missing columns in monthly data: {missing}"

# ---------- 3. Baseline aggregation per pincode ----------
baseline = (
    df
    .groupby('pincode')
    .agg(
        median_monthly_total=('total_monthly_count', 'median'),
        q1_monthly_total=('total_monthly_count', lambda x: x.quantile(0.25)),
        q3_monthly_total=('total_monthly_count', lambda x: x.quantile(0.75)),
        median_bio=('bio_updates', 'median'),
        median_demo=('demo_updates', 'median'),
        median_enroll=('enrollments', 'median'),
        num_months=('month', 'nunique')
    )
    .reset_index()
)

# ---------- 4. Interquartile range ----------
baseline['iqr_monthly_total'] = (
    baseline['q3_monthly_total'] - baseline['q1_monthly_total']
)

# ---------- 5. Safe stream ratios ----------
# NOTE: replace(0,1) is intentional to avoid divide-by-zero
baseline['bio_demo_ratio'] = (
    baseline['median_bio'] /
    baseline['median_demo'].replace(0, 1)
)

baseline['enroll_demo_ratio'] = (
    baseline['median_enroll'] /
    baseline['median_demo'].replace(0, 1)
)

# ---------- 6. Volatility indicator ----------
baseline['iqr_ratio'] = (
    baseline['iqr_monthly_total'] /
    baseline['median_monthly_total'].replace(0, 1)
)

# ---------- 7. Final sanity checks ----------
assert (baseline['num_months'] >= 3).all(), \
    "Some pincodes have <3 months of data (baseline unreliable)"

# ---------- 8. Save baseline ----------
baseline.to_csv(
    "data/processed/pincode_baseline_stats.csv",
    index=False
)

print("======================================")
print("STEP 2 COMPLETE")
print(f"Total pincodes processed: {len(baseline)}")
print("Baseline saved to: data/processed/pincode_baseline_stats.csv")
print("======================================")
