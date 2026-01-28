import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# --- Configuração do Banco ---

load_dotenv(override=True)

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

CONN_STRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Configuração de Caminhos ---
# Apontando para onde o treat.py salvou o arquivo limpo
PROCESSED_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/processed/pib"
    )
)

def load_pib():
    print("--- 3. Carga: Enviando PIB para o Postgres ---")
    
    input_path = os.path.join(PROCESSED_DIR, "pib_clean.csv")
    
    if not os.path.exists(input_path):
        print(f"[ERROR] Arquivo processado não encontrado em {input_path}")
        print("Dica: Rode o treat.py primeiro.")
        return

    try:
        # 1. Criar conexão
        engine = create_engine(CONN_STRING)
        
        # 2. Ler o CSV limpo
        df = pd.read_csv(input_path)
        table_name = 'tb_pib'
        
        print(f"[INFO] Carregando {len(df)} linhas na tabela '{table_name}'...")
        
        # 3. Salvar no banco (substitui a tabela se já existir)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        print("[SUCCESS] Tabela carregada com sucesso.")
        
    except Exception as e:
        print(f"[ERROR] Erro de Banco de Dados: {e}")

if __name__ == "__main__":
    load_pib()