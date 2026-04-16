import os
import shutil
import time
import sys
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from pipelines.inss.download import run as download_inss
    from pipelines.inss.extract import run as extrair_inss
    from pipelines.inss.spark_processor import run as processar_inss
    from pipelines.inss import load as upload_s3
except ImportError as e:
    print(f"Erro de Importacao: {e}")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Corrige o caminho para a pasta data na raiz do projeto
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../data"))

def reset_folders():
    # Só precisamos de resetar a pasta Gold para o INSS nesta arquitetura
    for subfolder in ["gold/inss"]:
        path = os.path.join(DATA_DIR, subfolder)
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path, exist_ok=True)

def clean_temp_files():
    for subfolder in ["raw/inss", "extracted/inss"]:
        path = os.path.join(DATA_DIR, subfolder)
        if os.path.exists(path):
            shutil.rmtree(path)
        # Não recriamos aqui. Deixamos as funções (download/extract) criarem quando precisarem.

def run_pipeline():
    start = time.time()
    print("A iniciar Pipeline do INSS (Macrodados)...")
    
    # Limpa tudo antes de começar para evitar sobreposição de ficheiros
    reset_folders()
    clean_temp_files()

    print("\n[STEP 1] Geração de Base (Download) API INSS")
    raw_path = download_inss()
    
    if raw_path:
        print("\n[STEP 2] Extração e Limpeza dos Dados")
        extracted_path = extrair_inss(raw_path)      
          
        if extracted_path:
            print(f"\n[STEP 3] Processamento Spark")
            processar_inss(extracted_path)

            print("\n[STEP 4] Upload para AWS S3")
            s3_path = upload_s3.run()
            print(f"Dados enviados com sucesso para: {s3_path}")
            
            print("\n[STEP 5] Limpeza de Ficheiros Temporários")
            clean_temp_files()
            
    # Calculamos o tempo em segundos, pois agora será muito rápido
    tempo = time.time() - start
    print(f"\nPipeline INSS finalizada com sucesso em {tempo:.2f} segundos!")

if __name__ == "__main__":
    run_pipeline()