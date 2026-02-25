import os
import shutil
import time
import sys
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from pipelines.orcamentos.download import run as download_orcamentos
    from pipelines.orcamentos.extract import run as extrair_orcamentos
    from pipelines.orcamentos.spark_processor import run as processar_orcamentos_spark
    from pipelines.orcamentos.load import run as upload_s3_module
except ImportError as e:
    print(f"Erro de Importacao: {e}")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")

def reset_orcamentos_folders():
    folders_to_reset = ["bronze/orcamentos", "silver/orcamentos", "gold/orcamentos"]
    for subfolder in folders_to_reset:
        path = os.path.join(DATA_DIR, subfolder)
        if os.path.exists(path):
            try:
                shutil.rmtree(path)
                print(f"Pasta limpa: {subfolder}")
            except Exception as e:
                print(f"Erro ao limpar {subfolder}: {e}")

def clean_temp_files():
    folders_to_clean = ["raw/orcamentos", "extracted/orcamentos"]
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
    print("Iniciando Pipeline Orcamentos...")
    
    reset_orcamentos_folders()

    print("\n[STEP 1] Download")
    raw_dir = download_orcamentos()
    
    if raw_dir:
        print("\n[STEP 2] Extracao")
        extracted_dir = extrair_orcamentos(raw_dir)
        
        if extracted_dir:
            print(f"\n[STEP 3] Processamento Spark")
            processar_orcamentos_spark(extracted_dir)
            
            print("\n[STEP 4] Upload S3")
            upload_s3_module()
            
            print("\n[STEP 5] Limpeza")
            clean_temp_files()
            
    tempo = (time.time() - start) / 60
    print(f"\nPipeline finalizada em {tempo:.2f} minutos")

if __name__ == "__main__":
    run_pipeline()