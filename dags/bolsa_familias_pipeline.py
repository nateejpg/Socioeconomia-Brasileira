import os
import shutil
import time
import sys
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from pipelines.bolsa_familia.download import download_zip as download_bf
    from pipelines.auxilio_brasil.download import download_data as download_ab
    from pipelines.novo_bolsa_familia.download import download_data as download_nb

    from pipelines.bolsa_familia.extract import extrair_dados
    from pipelines.auxilio_brasil.extract import extrair_dados_auxilio
    from pipelines.novo_bolsa_familia.extract import extrair_dados_novo_bolsa
    
    from pipelines.bolsa_familia.spark_processor import processar_com_spark as processar_bf
    from pipelines.auxilio_brasil.spark_processor import processar_auxilio_com_spark as processar_ab
    from pipelines.novo_bolsa_familia.spark_processor import processar_novo_bolsa_com_spark as processar_nb
    
    from pipelines.bolsa_familia import load as load_bf
    from pipelines.auxilio_brasil import load as load_ab
    from pipelines.novo_bolsa_familia import load as load_nb
    
except ImportError as e:
    print(f"Erro de Importacao: {e}")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")

def clean_environment():
    # Limpeza apenas de arquivos temporarios, nao da Gold
    paths_to_clean = [
        "bronze/bolsa_familia", "bronze/auxilio_brasil", "bronze/novo_bolsa_familia",
        "silver/bolsa_familia", "silver/auxilio_brasil", "silver/novo_bolsa_familia",
        "extracted/bolsa_familia", "extracted/auxilio_brasil", "extracted/novo_bolsa_familia",
        "raw/temp_download"
    ]
    
    print("\nIniciando limpeza PRE-EXECUCAO...")
    for subfolder in paths_to_clean:
        path = os.path.join(DATA_DIR, subfolder)
        if os.path.exists(path):
            try: shutil.rmtree(path)
            except: pass
    
    os.makedirs(os.path.join(DATA_DIR, "gold/fato_bolsa_familia"), exist_ok=True)

def processar_ciclo_mensal(ano, mes, programa):
    zip_path = None
    
    if programa == 'bolsa_antigo':
        zip_path = download_bf(str(ano), f"{mes:02d}")
        func_extract = extrair_dados
        func_spark = processar_bf
    elif programa == 'auxilio':
        zip_path = download_ab(ano, mes)
        func_extract = extrair_dados_auxilio
        func_spark = processar_ab
    elif programa == 'novo_bolsa':
        zip_path = download_nb(ano, mes)
        func_extract = extrair_dados_novo_bolsa
        func_spark = processar_nb

    if not zip_path or not os.path.exists(zip_path): return

    csv_path = func_extract(zip_path)
    
    if csv_path:
        print(f"Processando {programa}...")
        func_spark(csv_path)
        try: os.remove(csv_path)
        except: pass
    
    try: os.remove(zip_path)
    except: pass

def run_pipeline():
    start = time.time()
    clean_environment()
    print("Iniciando pipeline modular...")

    for ano in range(2013, 2022):
        for mes in range(1, 13):
            if ano == 2021 and mes > 10: break
            processar_ciclo_mensal(ano, mes, 'bolsa_antigo')
    
    print("\n--- Checkpoint: Upload Bolsa Família ---")
    load_bf.run()

    for ano in range(2021, 2023):
        for mes in range(1, 13):
            if ano == 2021 and mes < 11: continue
            processar_ciclo_mensal(ano, mes, 'auxilio')
            
    print("\n--- Checkpoint: Upload Auxílio Brasil ---")
    load_ab.run()

    for ano in range(2023, 2027): 
        for mes in range(1, 13):
            processar_ciclo_mensal(ano, mes, 'novo_bolsa')
            
    print("\n--- Checkpoint: Upload Novo Bolsa ---")
    load_nb.run()
    
    tempo = (time.time() - start) / 60
    print(f"\nPipeline finalizada em {tempo:.2f} minutos")

if __name__ == "__main__":
    run_pipeline()