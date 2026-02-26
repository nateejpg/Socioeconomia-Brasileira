import os
import shutil
import time
import sys
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from pipelines.desemprego.download import run as download_desemprego
    from pipelines.desemprego.extract import run as extrair_desemprego
    from pipelines.desemprego.spark_processor import run as processar_desemprego
    from pipelines.desemprego import load as upload_s3
except ImportError as e:
    print(f"Erro de Importacao: {e}")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "../data")

def reset_folders():
    for subfolder in ["bronze/desemprego", "silver/desemprego", "gold/desemprego"]:
        path = os.path.join(DATA_DIR, subfolder)
        if os.path.exists(path):
            shutil.rmtree(path)

def clean_temp_files():
    for subfolder in ["raw/desemprego", "extracted/desemprego"]:
        path = os.path.join(DATA_DIR, subfolder)
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path, exist_ok=True)

def run_pipeline():
    start = time.time()
    print("Iniciando Pipeline Desemprego (API SIDRA IBGE)...")
    
    reset_folders()

    print("\n[STEP 1] Download API IBGE")
    json_path = download_desemprego()
    
    if json_path:
        print("\n[STEP 2] Extração JSON -> CSV")
        csv_path = extrair_desemprego()
        
        if csv_path:
            print(f"\n[STEP 3] Processamento Spark")
            processar_desemprego(csv_path)
            
            try: os.remove(csv_path)
            except: pass

            print("\n[STEP 4] Upload para AWS S3")
            upload_s3.run()
            
            print("\n[STEP 5] Limpeza de Arquivos Temporários")
            clean_temp_files()
            
    tempo = (time.time() - start) / 60
    print(f"\nPipeline Desemprego finalizada com sucesso em {tempo:.2f} minutos")

if __name__ == "__main__":
    run_pipeline()