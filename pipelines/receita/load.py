import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(override=True)

# --- Configuration ---
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

# Connection String
CONN_STRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

AGGREGATED_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/aggregated/receita"
    )
)

def get_engine():
    return create_engine(CONN_STRING)

def load_to_postgres():
    engine = get_engine()
    
    print(f"Loading data from: {AGGREGATED_DIR}")

    # Map filename -> Target Table Name
    files_to_load = {
        'receita_anual.csv': 'tb_receita_anual',
        'receita_mensal.csv': 'tb_receita_mensal'
    }

    print("--- Starting Database Load ---")

    for filename, table_name in files_to_load.items():
        file_path = os.path.join(AGGREGATED_DIR, filename)
        
        if os.path.exists(file_path):
            try:
                print(f"Reading {filename}...")
                df = pd.read_csv(file_path)
                
                print(f"Loading {len(df)} rows into '{table_name}'...")
                df.to_sql(table_name, engine, if_exists='replace', index=False)
                
                print(f"Success: {table_name} loaded.")
            except Exception as e:
                print(f"Error loading {table_name}: {e}")
        else:
            print(f"Warning: File not found at {file_path}")

    print("--- Load Process Completed ---")

if __name__ == "__main__":
    load_to_postgres()