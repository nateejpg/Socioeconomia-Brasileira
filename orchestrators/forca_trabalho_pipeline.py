import os
import shutil
import time
import sys
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from pipelines.forca_trabalho.download import run as download_ft
    from pipelines.forca_trabalho.extract import run as extrair_ft
    from pipelines.forca_trabalho.spark_processor import run as processar_ft_spark
    from pipelines.forca_trabalho import load as upload_s3_module
except ImportError as e:
    print(f"Erro de Importacao: {e}")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")

def reset_forca_trabalho_folders():
    folders_to_reset = [
        "bronze/forca_trabalho",
        "silver/forca_trabalho",
        "gold/forca_trabalho"
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
        "raw/forca_trabalho",
        "extracted/forca_trabalho"
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
    
    print("Iniciando Pipeline Forca de Trabalho (Medallion Architecture)...")
    
    reset_forca_trabalho_folders()

    print("\n[STEP 1] Download")
    raw_dir = download_ft()
    
    if raw_dir:
        print("\n[STEP 2] Extração e Conversão")
        extracted_dir = extrair_ft(raw_dir)
        
        if extracted_dir:
            print(f"\n[STEP 3] Processamento Spark")
            processar_ft_spark(extracted_dir)

            print("\n[STEP 4] Upload da camada Gold para S3")
            upload_s3_module.run()
            
            print("\n[STEP 5] Limpeza de arquivos Raw/Extracted")
            clean_temp_files()
        else:
            print("ERRO: O diretorio de extracao nao foi retornado.")
    else:
        print("ERRO: O diretorio raw nao foi retornado.")

    tempo = (time.time() - start) / 60
    print(f"\nPipeline Forca de Trabalho finalizada com sucesso em {tempo:.2f} minutos")

if __name__ == "__main__":
    run_pipeline()