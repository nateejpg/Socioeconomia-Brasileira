import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from pipelines.bolsa_familia.extract import extrair_dados
from pipelines.bolsa_familia.spark_processor import processar_com_spark

BASE_DIR = os.path.dirname(__file__)
RAW_ZIP_FOLDER = os.path.abspath(os.path.join(BASE_DIR, "../../data/raw/bolsa_familia"))

def processar_historico():
    if not os.path.exists(RAW_ZIP_FOLDER):
        print(f"Pasta nao encontrada: {RAW_ZIP_FOLDER}")
        return

    arquivos_zip = sorted([f for f in os.listdir(RAW_ZIP_FOLDER) if f.endswith(".zip")])
    total = len(arquivos_zip)
    
    print(f"Iniciando processamento com Spark de {total} arquivos...")

    for i, nome_arquivo in enumerate(arquivos_zip):
        caminho_zip = os.path.join(RAW_ZIP_FOLDER, nome_arquivo)
        caminho_csv = None
        
        print(f"[{i+1}/{total}] Processando: {nome_arquivo}")

        try:
            caminho_csv = extrair_dados(caminho_zip)
            
            if caminho_csv:
                processar_com_spark(caminho_csv)
                print("Processamento Spark concluido com sucesso.")

        except Exception as e:
            print(f"Erro critico: {e}")

        finally:
            if caminho_csv and os.path.exists(caminho_csv):
                try:
                    os.remove(caminho_csv)
                except:
                    pass

if __name__ == "__main__":
    processar_historico()