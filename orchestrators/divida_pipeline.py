import os
import shutil
import time
import sys
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
try:
    from pipelines.divida.download import run as download_divida
    from pipelines.divida.extract import run as extrair_divida
    from pipelines.divida.spark_processor import run as processar_divida_spark
    from pipelines.divida import load as upload_s3_module
except ImportError as e:
    print(f"Erro de Importacao: {e}")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "../data")

def reset_divida_folders():
    for subfolder in ["bronze/divida", "silver/divida", "gold/divida"]:
        path = os.path.join(DATA_DIR, subfolder)
        if os.path.exists(path):
            shutil.rmtree(path)

def clean_temp_files():
    for subfolder in ["raw/divida", "extracted/divida"]:
        path = os.path.join(DATA_DIR, subfolder)
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path, exist_ok=True)

def run_pipeline():
    start = time.time()
    print("Iniciando Pipeline Dívida Pública (Série 13762)...")
    
    reset_divida_folders()

    print("\n[STEP 1] Download API BCB")
    download_divida()
    
    print("\n[STEP 2] Extração JSON -> CSV")
    csv_path = extrair_divida()
    
    if csv_path:
        print(f"\n[STEP 3] Processamento Spark")
        processar_divida_spark(csv_path)
        
        try: os.remove(csv_path)
        except: pass

        print("\n[STEP 4] Upload para AWS S3 (Fresh Start)")
        upload_s3_module.run()
        
        print("\n[STEP 5] Limpeza de arquivos Raw/Extracted")
        clean_temp_files()
        
    tempo = (time.time() - start) / 60
    print(f"\nPipeline Dívida finalizada com sucesso em {tempo:.2f} minutos")

if __name__ == "__main__":
    run_pipeline()