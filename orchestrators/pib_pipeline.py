import os
import shutil
import time
import sys
from dotenv import load_dotenv

load_dotenv()

# Ajustado para voltar 1 nivel (de dags/ para a raiz)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from pipelines.pib.download import run as download_pib
    from pipelines.pib.extract import run as extrair_pib
    from pipelines.pib.spark_processor import run as processar_pib_spark
    from pipelines.pib import load as upload_s3_module
except ImportError as e:
    print(f"Erro de Importacao: {e}")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")

def reset_pib_folders():
    folders_to_reset = [
        "bronze/pib",
        "silver/pib",
        "gold/pib"
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
        "raw/pib",
        "extracted/pib"
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
    
    print("Iniciando Pipeline PIB (Medallion Architecture)...")
    
    reset_pib_folders()

    print("\n[STEP 1] Download")
    download_pib()
    
    print("\n[STEP 2] Extração e Conversão")
    csv_path = extrair_pib()
    
    if csv_path:
        print(f"\n[STEP 3] Processamento Spark")
        processar_pib_spark(csv_path)
        
        # Remove apenas o CSV gerado na extração para liberar disco
        try: 
            os.remove(csv_path)
            print("CSV intermediário removido.")
        except: 
            pass

        print("\n[STEP 4] Upload das camadas Bronze, Silver e Gold para S3")
        upload_s3_module.run()
        
        print("\n[STEP 5] Limpeza de arquivos Raw/Extracted")
        clean_temp_files()
        
    else:
        print("ERRO: O arquivo CSV não foi gerado na extração.")

    tempo = (time.time() - start) / 60
    print(f"\nPipeline PIB finalizada com sucesso em {tempo:.2f} minutos")

if __name__ == "__main__":
    run_pipeline()