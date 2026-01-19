import pandas as pd
import os

# ==========================================
# CONFIGURATION (ROBUST PATHS)
# ==========================================
# Get the absolute path of the folder containing THIS script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Define paths relative to the script, not the terminal
# resolves to: .../Hackathon UIDAI/data/raw
RAW_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '../data/raw'))
OUTPUT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '../data/processed'))

# Exact filenames
FILE_BIO = 'Aadhaar Biometric Monthly Update Data.csv'
FILE_DEMO = 'Aadhar Demographic Updates for Thane.csv'
FILE_ENROL = 'Aadhar Enrollment Dataset for Thane.csv'

# ==========================================
# HELPER FUNCTION
# ==========================================
def load_and_prep(filename, date_col='date'):
    path = os.path.join(RAW_DIR, filename)
    
    # Debug print to show exactly where we are looking
    print(f"Looking for file at: {path}")
    
    if not os.path.exists(path):
        raise FileNotFoundError(f"CRITICAL ERROR: Could not find file at {path}")
    
    print(f"Loading {filename}...")
    df = pd.read_csv(path)
    
    # Standardize Date format (DD-MM-YYYY)
    df['date'] = pd.to_datetime(df[date_col], format='%d-%m-%Y')
    
    # Create Monthly Period (e.g., '2025-03')
    df['month'] = df['date'].dt.to_period('M')
    return df

# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    print("--- STARTING DATA PIPELINE ---")
    
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Load Data
    try:
        bio_df = load_and_prep(FILE_BIO)
        demo_df = load_and_prep(FILE_DEMO)
        enrol_df = load_and_prep(FILE_ENROL)
    except Exception as e:
        print("\n!!! PATH ERROR DETECTED !!!")
        print(e)
        return

    print("\n--- Aggregating Datasets ---")

    # 2. Process Biometric Data
    bio_agg = bio_df.groupby(['pincode', 'month'])[['bio_age_5_17', 'bio_age_17_']].sum().reset_index()
    bio_agg.rename(columns={
        'bio_age_5_17': 'bio_child', 
        'bio_age_17_': 'bio_adult'
    }, inplace=True)
    bio_agg['bio_total'] = bio_agg['bio_child'] + bio_agg['bio_adult']

    # 3. Process Demographic Data
    demo_agg = demo_df.groupby(['pincode', 'month'])[['demo_age_5_17', 'demo_age_17_']].sum().reset_index()
    demo_agg.rename(columns={
        'demo_age_5_17': 'demo_child', 
        'demo_age_17_': 'demo_adult'
    }, inplace=True)
    demo_agg['demo_total'] = demo_agg['demo_child'] + demo_agg['demo_adult']

    # 4. Process Enrollment Data
    enrol_agg = enrol_df.groupby(['pincode', 'month'])[['age_0_5', 'age_5_17', 'age_18_greater']].sum().reset_index()
    enrol_agg.rename(columns={
        'age_0_5': 'enrol_infant', 
        'age_5_17': 'enrol_child', 
        'age_18_greater': 'enrol_adult'
    }, inplace=True)
    enrol_agg['enrol_total'] = enrol_agg['enrol_infant'] + enrol_agg['enrol_child'] + enrol_agg['enrol_adult']

    # 5. Merge All Streams (Outer Join)
    print("Merging datasets...")
    master = bio_agg.merge(demo_agg, on=['pincode', 'month'], how='outer')
    master = master.merge(enrol_agg, on=['pincode', 'month'], how='outer')

    # 6. Clean Up
    master = master.fillna(0)
    master = master.sort_values(by=['pincode', 'month'])

    # 7. Save to Processed Folder
    output_path = os.path.join(OUTPUT_DIR, 'master_aadhaar_thane.csv')
    master.to_csv(output_path, index=False)
    
    print("\n" + "="*40)
    print(f"SUCCESS! Master Dataset saved to:")
    print(f"{output_path}")
    print("="*40)

    # 8. Verification Output
    print("\n--- PREVIEW (First 5 Rows) ---")
    print(master.head())
    print("\n--- DATA STRUCTURE ---")
    print(master.info())

if __name__ == "__main__":
    main()