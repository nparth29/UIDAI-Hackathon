import pandas as pd

# -------------------------------------------------
# STEP 3: Monthly Spike Anomaly Detection
# -------------------------------------------------

# ---------- 1. Load processed monthly data ----------
df = pd.read_csv("data/processed/monthly_pin_level.csv")

# ---------- 2. Load baseline stats ----------
baseline = pd.read_csv("data/processed/pincode_baseline_stats.csv")

# ---------- 3. Merge baseline into monthly data ----------
df = df.merge(baseline, on="pincode", how="left")

# ---------- 4. Sanity check ----------
required_cols = [
    'median_monthly_total',
    'q3_monthly_total',
    'iqr_monthly_total',
    'iqr_ratio'
]

missing = [c for c in required_cols if c not in df.columns]
assert len(missing) == 0, f"Missing baseline columns: {missing}"

# ---------- 5. Compute spike threshold (IQR rule) ----------
df['spike_threshold'] = (
    df['q3_monthly_total'] + 3 * df['iqr_monthly_total']
)

# ---------- 6. Initial spike flag ----------
df['is_monthly_spike'] = (
    df['total_monthly_count'] > df['spike_threshold']
)

# ---------- 7. Volatility guardrail ----------
# Very volatile pincodes require stronger evidence
df.loc[
    (df['iqr_ratio'] > 2.5),
    'is_monthly_spike'
] = False

# ---------- 8. Extract only anomalies ----------
spike_anomalies = df[df['is_monthly_spike']].copy()

# ---------- 9. Add explanation text ----------
spike_anomalies['reason'] = (
    "Monthly count exceeded baseline (Q3 + 3×IQR)"
)

# ---------- 10. Keep only useful columns ----------
spike_anomalies = spike_anomalies[
    [
        'pincode',
        'month',
        'total_monthly_count',
        'median_monthly_total',
        'iqr_monthly_total',
        'spike_threshold',
        'iqr_ratio',
        'reason'
    ]
]

# ---------- 11. Save result ----------
spike_anomalies.to_csv(
    "data/processed/monthly_spike_anomalies.csv",
    index=False
)

print("======================================")
print(f"STEP 3 COMPLETE")
print(f"Total spike anomalies found: {len(spike_anomalies)}")
print("Saved to: data/processed/monthly_spike_anomalies.csv")
print("======================================")
