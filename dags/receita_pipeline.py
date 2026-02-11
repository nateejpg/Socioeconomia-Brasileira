import os
import shutil
import time
import sys
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from pipelines.receita.download import download_receita
    from pipelines.receita.extract import extrair_receita
    from pipelines.receita.spark_processor import processar_receita_spark
    from pipelines.receita import load as upload_s3
except ImportError as e:
    print(f"Erro de Importacao: {e}")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")

def reset_receita_folders():
    folders_to_reset = [
        "bronze/receita",
        "silver/receita",
        "gold/receita"
    ]
    
    for subfolder in folders_to_reset:
        path = os.path.join(DATA_DIR, subfolder)
        if os.path.exists(path):
            try:
                shutil.rmtree(path)
            except Exception as e:
                print(f"Erro ao limpar {subfolder}: {e}")

def clean_temp_files():
    folders_to_clean = [
        "raw/receita",
        "extracted/receita"
    ]
    
    for subfolder in folders_to_clean:
        path = os.path.join(DATA_DIR, subfolder)
        if os.path.exists(path):
            try:
                shutil.rmtree(path)
                os.makedirs(path, exist_ok=True)
            except Exception as e:
                print(f"Erro ao limpar {subfolder}: {e}")

def run_pipeline():
    start = time.time()
    
    print("Iniciando Pipeline Receita Federal...")
    
    reset_receita_folders()

    anos = range(2013, 2026) 

    for ano in anos:
        zip_path = download_receita(ano)
        
        if zip_path:
            csv_path = extrair_receita(zip_path)
            
            if csv_path:
                print(f"Processando Spark: {ano}")
                processar_receita_spark(csv_path)
                
                try: os.remove(csv_path)
                except: pass

            try: os.remove(zip_path)
            except: pass

    print("\nIniciando Upload para S3...")
    
    upload_s3.run_upload()
    
    clean_temp_files()
    
    tempo = (time.time() - start) / 60
    print(f"\nPipeline Receita finalizada em {tempo:.2f} minutos")

if __name__ == "__main__":
    run_pipeline()