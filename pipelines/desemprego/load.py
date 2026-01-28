import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(override=True)

# --- Database Configuration ---
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

CONN_STRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

PROCESSED_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/processed/desemprego"
    )
)

def load_unemployment():
    print(f"--- 3. Loading: Unemployment Data to Postgres ---")
    
    input_path = os.path.join(PROCESSED_DIR, "unemployment_clean.csv")
    
    if not os.path.exists(input_path):
        print(f" Error: Processed file not found at {input_path}")
        print("Tip: Run treat.py first.")
        return

    try:
        # 1. Create Engine
        engine = create_engine(CONN_STRING)
        
        # 2. Read Clean Data
        df = pd.read_csv(input_path)
        
        # 3. Load to Database
        table_name = 'tb_desemprego'
        
        print(f" Loading {len(df)} rows into table '{table_name}'...")
        
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        print(" Success: Table loaded.")
        
    except Exception as e:
        print(f" Database Error: {e}")

if __name__ == "__main__":
    load_unemployment()