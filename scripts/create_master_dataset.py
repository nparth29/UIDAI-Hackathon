"""
create_master_dataset.py
========================
UIDAI Aadhaar Fraud Intelligence System — Thane District
---------------------------------------------------------
PURPOSE:
    Merges the 3 raw Aadhaar data streams (Biometric, Demographic,
    Enrollment) into a single clean monthly master dataset.

INPUT:
    data/raw/Aadhaar Biometric Monthly Update Data.csv
    data/raw/Aadhar Demographic Updates for Thane.csv
    data/raw/Aadhar Enrollment Dataset for Thane.csv

OUTPUT:
    data/processed/master_aadhaar_thane.csv

HOW TO RUN:
    From the project root directory:
        python scripts/create_master_dataset.py
"""

import os
import pandas as pd

# ─────────────────────────────────────────────
# PATHS  (all relative to project root)
# ─────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

RAW_DIR      = os.path.join(PROJECT_ROOT, 'data', 'raw')
OUTPUT_DIR   = os.path.join(PROJECT_ROOT, 'data', 'processed')

FILE_BIO     = 'Aadhaar Biometric Monthly Update Data.csv'
FILE_DEMO    = 'Aadhar Demographic Updates for Thane.csv'
FILE_ENROL   = 'Aadhar Enrollment Dataset for Thane.csv'
OUTPUT_FILE  = 'master_aadhaar_thane.csv'

# ─────────────────────────────────────────────
# STEP 1 — LOAD & VALIDATE
# ─────────────────────────────────────────────
def load_raw(filename):
    path = os.path.join(RAW_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")
    print(f"  Loading  →  {filename}")
    df = pd.read_csv(path)

    # Parse date robustly
    df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
    assert df['date'].isna().sum() == 0, f"Date parse failures in {filename}"

    # Create YYYY-MM period key
    df['month'] = df['date'].dt.to_period('M').dt.to_timestamp()
    return df


# ─────────────────────────────────────────────
# STEP 2 — AGGREGATE TO MONTHLY LEVEL
# ─────────────────────────────────────────────
def aggregate(df, value_cols):
    """Group by pincode + month, sum all value columns."""
    return (
        df.groupby(['pincode', 'month'], as_index=False)[value_cols]
          .sum()
    )


# ─────────────────────────────────────────────
# STEP 3 — MERGE ALL 3 STREAMS
# ─────────────────────────────────────────────
def build_master(bio_agg, demo_agg, enrol_agg):
    master = bio_agg.merge(demo_agg,   on=['pincode', 'month'], how='outer')
    master = master.merge(enrol_agg,   on=['pincode', 'month'], how='outer')
    return master


# ─────────────────────────────────────────────
# STEP 4 — CLEAN & STANDARDIZE
# ─────────────────────────────────────────────
def clean(df):
    # Fill missing values with 0 (center had no activity in that stream)
    df = df.fillna(0)

    # Rename columns to clear, consistent names
    df = df.rename(columns={
        'bio_age_5_17'    : 'bio_child',
        'bio_age_17_'     : 'bio_adult',
        'demo_age_5_17'   : 'demo_child',
        'demo_age_17_'    : 'demo_adult',
        'age_0_5'         : 'enrol_infant',
        'age_5_17'        : 'enrol_child',
        'age_18_greater'  : 'enrol_adult',
    })

    # Add stream totals
    df['bio_total']   = df['bio_child']   + df['bio_adult']
    df['demo_total']  = df['demo_child']  + df['demo_adult']
    df['enrol_total'] = df['enrol_infant']+ df['enrol_child'] + df['enrol_adult']

    # Format month as YYYY-MM string
    df['month'] = df['month'].dt.strftime('%Y-%m')

    # Sort
    df = df.sort_values(['pincode', 'month']).reset_index(drop=True)

    # Final column order
    df = df[[
        'pincode', 'month',
        'bio_child', 'bio_adult', 'bio_total',
        'demo_child', 'demo_adult', 'demo_total',
        'enrol_infant', 'enrol_child', 'enrol_adult', 'enrol_total'
    ]]
    return df


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("\n" + "="*55)
    print("  UIDAI Master Dataset Pipeline")
    print("="*55)

    # Load
    print("\n[1/4] Loading raw files...")
    bio   = load_raw(FILE_BIO)
    demo  = load_raw(FILE_DEMO)
    enrol = load_raw(FILE_ENROL)

    # Aggregate
    print("\n[2/4] Aggregating to monthly level...")
    bio_agg   = aggregate(bio,   ['bio_age_5_17', 'bio_age_17_'])
    demo_agg  = aggregate(demo,  ['demo_age_5_17', 'demo_age_17_'])
    enrol_agg = aggregate(enrol, ['age_0_5', 'age_5_17', 'age_18_greater'])

    # Merge
    print("\n[3/4] Merging all 3 streams...")
    master = build_master(bio_agg, demo_agg, enrol_agg)

    # Clean
    print("\n[4/4] Cleaning and standardizing...")
    master = clean(master)

    # Validate
    dupes = master.duplicated(subset=['pincode', 'month']).sum()
    assert dupes == 0, f"Duplicate pincode-month pairs found: {dupes}"
    assert master.isnull().sum().sum() == 0, "Null values found in master dataset"

    # Save
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    master.to_csv(output_path, index=False)

    # Summary
    print("\n" + "="*55)
    print("  SUCCESS")
    print("="*55)
    print(f"  Rows         : {len(master)}")
    print(f"  Columns      : {len(master.columns)}")
    print(f"  Pincodes     : {master['pincode'].nunique()}")
    print(f"  Months       : {sorted(master['month'].unique())}")
    print(f"  Saved to     : {output_path}")
    print("="*55)
    print("\nPreview (first 5 rows):")
    print(master.head().to_string())
    print()


if __name__ == "__main__":
    main()