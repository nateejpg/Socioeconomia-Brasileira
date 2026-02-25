import os
import shutil
import time
import sys
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from pipelines.idh.download import run as download_idh
    from pipelines.idh.extract import run as extrair_idh
    from pipelines.idh.spark_processor import run as processar_idh_spark
    from pipelines.idh.load import run as upload_s3_module
except ImportError as e:
    print(f"Erro de Importacao: {e}")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")

def reset_idh_folders():
    folders_to_reset = [
        "bronze/idh",
        "silver/idh",
        "gold/idh"
    ]
    
    for subfolder in folders_to_reset:
        path = os.path.join(DATA_DIR, subfolder)
        if os.path.exists(path):
            try:
                shutil.rmtree(path)
                print(f"Pasta de camadas limpa: {subfolder}")
            except Exception as e:
                print(f"Erro ao limpar {subfolder}: {e}")

def clean_temp_files():
    folders_to_clean = [
        "raw/idh",
        "extracted/idh"
    ]
    
    for subfolder in folders_to_clean:
        path = os.path.join(DATA_DIR, subfolder)
        if os.path.exists(path):
            try:
                shutil.rmtree(path)
                os.makedirs(path, exist_ok=True)
                print(f"Temp limpo: {subfolder}")
            except Exception as e:
                print(f"Erro ao limpar {subfolder}: {e}")

def run_pipeline():
    start = time.time()
    
    print("Iniciando Pipeline IDH (Medallion Architecture)...")
    
    reset_idh_folders()

    print("\n[STEP 1] Download")
    raw_csv_path = download_idh()
    
    if raw_csv_path:
        print("\n[STEP 2] Extração e Conversão")
        parquet_path = extrair_idh(raw_csv_path)
        
        if parquet_path:
            print(f"\n[STEP 3] Processamento Spark")
            processar_idh_spark(parquet_path)
            
            print("\n[STEP 4] Upload das camadas Bronze, Silver e Gold para S3")
            upload_s3_module()
            
            print("\n[STEP 5] Limpeza de arquivos Raw/Extracted")
            clean_temp_files()
            
    tempo = (time.time() - start) / 60
    print(f"\nPipeline IDH finalizada com sucesso em {tempo:.2f} minutos")

if __name__ == "__main__":
    run_pipeline()