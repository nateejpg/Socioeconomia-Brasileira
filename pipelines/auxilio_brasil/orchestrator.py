import sys
import os

sys.path.append(os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), 
        "../../")
            ))

from pipelines.auxilio_brasil.extract import extrair_dados_auxilio
from pipelines.auxilio_brasil.spark_processor import processar_auxilio_com_spark

BASE_DIR = os.path.dirname(__file__)
RAW_ZIP_FOLDER = os.path.abspath(os.path.join(BASE_DIR, "../../data/raw/auxilio_brasil"))

def run():
    if not os.path.exists(RAW_ZIP_FOLDER):
        print("Pasta de dados não encontrada.")
        return

    arquivos = sorted([f for f in os.listdir(RAW_ZIP_FOLDER) if f.endswith(".zip")])
    
    if not arquivos:
        print("Nenhum ZIP encontrado. O download já terminou?")
        return

    print(f"--- Processando {len(arquivos)} arquivos do Auxílio Brasil ---")

    for f in arquivos:
        caminho_zip = os.path.join(RAW_ZIP_FOLDER, f)
        
        try:
            # 1. Extrai
            caminho_csv = extrair_dados_auxilio(caminho_zip)
            
            if caminho_csv:
                # 2. Processa (Spark)
                processar_auxilio_com_spark(caminho_csv)
                
                # 3. Limpa (Apaga CSV para economizar espaço)
                try:
                    os.remove(caminho_csv)
                except:
                    pass
                print(" [OK] Ciclo completo.")
                
        except Exception as e:
            print(f" [ERRO] Falha em {f}: {e}")

if __name__ == "__main__":
    run()