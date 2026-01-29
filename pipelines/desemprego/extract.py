import pandas as pd
from pandas_datareader import wb
import os

EXTRACTED_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/extracted/desemprego"
    )
)

os.makedirs(EXTRACTED_DIR, exist_ok=True)

def extract_unemployment():
    print("--- 1. Extraction: Unemployment Data (World Bank) ---")
    
    try:
        # Fetching raw data for Brazil (BR)
        # ID: SL.UEM.TOTL.ZS (Unemployment, total % of labor force)
        df_raw = wb.download(
            indicator='SL.UEM.TOTL.ZS', 
            country=['BR'], 
            start=2000, 
            end=2026
        )
        
        # Ensure directory exists
        os.makedirs(EXTRACTED_DIR, exist_ok=True)
        
        # Define Output Path
        output_path = os.path.join(EXTRACTED_DIR, 'unemployment_wb_raw.csv')
        
        # Save RAW data (No renaming, no casting, no sorting)
        # We just save it exactly as the API gave it to us
        df_raw.to_csv(output_path)
        
        print(f" Success: Raw data saved to:")
        print(f"   {output_path}")
        
    except Exception as e:
        print(f" Error during extraction: {e}")

if __name__ == "__main__":
    extract_unemployment()