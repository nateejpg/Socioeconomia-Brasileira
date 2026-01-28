import pandas as pd
import os

EXTRACTED_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/extracted/desemprego"
    )
)

# 2. Define where the PROCESSED data will go
PROCESSED_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/processed/desemprego"
    )
)

os.makedirs(PROCESSED_DIR, exist_ok=True)

def treat_unemployment():
    print(f"--- 2. Transformation: Cleaning Unemployment Data ---")
    
    # Path to the input raw file
    input_path = os.path.join(EXTRACTED_DIR, "unemployment_wb_raw.csv")
    
    # Path to the output clean file
    output_path = os.path.join(PROCESSED_DIR, "unemployment_clean.csv")
    
    if not os.path.exists(input_path):
        print(f" Error: Raw file not found at {input_path}")
        return

    try:
        # 1. Read Raw Data
        df = pd.read_csv(input_path)
        
        # 2. Rename Columns 
        # API returns MultiIndex or 'date'/'value' depending on version.
        # We normalize common variations to our standard.
        df.rename(columns={
            'date': 'ano', 
            'value': 'taxa_desemprego',
            'SL.UEM.TOTL.ZS': 'taxa_desemprego', # If coming from wb.download directly
            'year': 'ano'
        }, inplace=True)
        
        # 3. Data Type Enforcing
        # Remove any non-numeric rows if they exist
        df = df[pd.to_numeric(df['ano'], errors='coerce').notnull()]
        
        df['ano'] = df['ano'].astype(int)
        df['taxa_desemprego'] = pd.to_numeric(df['taxa_desemprego'], errors='coerce')
        
        # 4. Filter relevant years (2003-2026 to match Bolsa Família)
        df = df[(df['ano'] >= 2003) & (df['ano'] <= 2026)]
        
        # 5. Round values (2 decimal places)
        df['taxa_desemprego'] = df['taxa_desemprego'].round(2)
        
        # 6. Sort by Year
        df = df.sort_values('ano', ascending=True)

        # 7. Save
        df.to_csv(output_path, index=False)
        
        print(f" Success: Cleaned data saved to:")
        print(f"   {output_path}")
        print(df.head())

    except Exception as e:
        print(f" Error during transformation: {e}")

if __name__ == "__main__":
    treat_unemployment()