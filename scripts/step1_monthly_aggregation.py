import pandas as pd

# ---------- 1. Load raw datasets ----------
bio = pd.read_csv("data/raw/Aadhaar Biometric Monthly Update Data.csv")
demo = pd.read_csv("data/raw/Aadhar Demographic Updates for Thane.csv")
enroll = pd.read_csv("data/raw/Aadhar Enrollment Dataset for Thane.csv")

# ---------- 2. Parse date correctly ----------
for df in [bio, demo, enroll]:
    df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')

# ---------- 3. Create month column ----------
for df in [bio, demo, enroll]:
    df['month'] = df['date'].dt.to_period('M').astype(str)

# ---------- 4. Aggregate each dataset monthly ----------

bio_monthly = (
    bio
    .groupby(['pincode', 'month'], as_index=False)
    .agg(
        bio_updates=('bio_age_5_17', 'sum')
    )
)
bio_monthly['bio_updates'] += bio.groupby(['pincode','month'])['bio_age_17_'].sum().values

demo_monthly = (
    demo
    .groupby(['pincode', 'month'], as_index=False)
    .agg(
        demo_updates=('demo_age_5_17', 'sum')
    )
)
demo_monthly['demo_updates'] += demo.groupby(['pincode','month'])['demo_age_17_'].sum().values

enroll_monthly = (
    enroll
    .groupby(['pincode', 'month'], as_index=False)
    .agg(
        enrollments=('age_0_5', 'sum')
    )
)
enroll_monthly['enrollments'] += (
    enroll.groupby(['pincode','month'])['age_5_17'].sum().values +
    enroll.groupby(['pincode','month'])['age_18_greater'].sum().values
)

# ---------- 5. Merge all three ----------
monthly = bio_monthly.merge(
    demo_monthly, on=['pincode','month'], how='outer'
).merge(
    enroll_monthly, on=['pincode','month'], how='outer'
)

# ---------- 6. Fill NaN with 0 ----------
monthly[['bio_updates','demo_updates','enrollments']] = (
    monthly[['bio_updates','demo_updates','enrollments']]
    .fillna(0)
    .astype(int)
)

# ---------- 7. Total count ----------
monthly['total_monthly_count'] = (
    monthly['bio_updates'] +
    monthly['demo_updates'] +
    monthly['enrollments']
)

# ---------- 8. Save ----------
monthly.to_csv(
    "data/processed/monthly_pin_level.csv",
    index=False
)

print("Step 1 complete: monthly_pin_level.csv created")
