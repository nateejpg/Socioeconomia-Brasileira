import os
import zipfile
import pandas as pd
import glob
import shutil

# --- DEFINIÇÃO DE CAMINHOS ---
RAW_FOLDER = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/raw/pib"
    )
)

EXTRACTED_FOLDER = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/extracted/pib"
    )
)

def run():
    # 1. Limpa a pasta de extração para começar do zero
    if os.path.exists(EXTRACTED_FOLDER):
        shutil.rmtree(EXTRACTED_FOLDER)
    os.makedirs(EXTRACTED_FOLDER)

    # 2. Busca o ZIP correto
    zip_files = [f for f in glob.glob(os.path.join(RAW_FOLDER, "*2010_2023*.zip")) if "xlsx" in f]
    
    if not zip_files:
        zip_files = glob.glob(os.path.join(RAW_FOLDER, "*2010_2023*.zip"))

    if not zip_files:
        print(f"Nenhum arquivo ZIP compatível encontrado em: {RAW_FOLDER}")
        return None

    arquivo_zip = zip_files[0]
    
    try:
        print(f"Extraindo: {os.path.basename(arquivo_zip)}...")
        
        with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
            zip_ref.extractall(EXTRACTED_FOLDER)
        
        # 3. Identifica a planilha extraída
        arquivos_planilha = glob.glob(os.path.join(EXTRACTED_FOLDER, "*.xlsx"))
        if not arquivos_planilha:
             arquivos_planilha = glob.glob(os.path.join(EXTRACTED_FOLDER, "*.xls*")) + glob.glob(os.path.join(EXTRACTED_FOLDER, "*.ods"))

        if not arquivos_planilha:
            return None
            
        arquivo_input = arquivos_planilha[0]
        nome_csv = "pib_municipios_2010_2023.csv"
        caminho_csv = os.path.join(EXTRACTED_FOLDER, nome_csv)
        
        print(f"Convertendo para CSV: {os.path.basename(arquivo_input)}")
        
        # 4. Converte
        if arquivo_input.endswith('.ods'):
            df = pd.read_excel(arquivo_input, engine='odf')
        else:
            df = pd.read_excel(arquivo_input)
            
        df.to_csv(caminho_csv, index=False, sep=';', encoding='utf-8')
        print(f"CSV gerado: {caminho_csv}")
        
        # 5. FAXINA: Remove o arquivo Excel original para não duplicar
        os.remove(arquivo_input)
        print(f"Arquivo original removido: {os.path.basename(arquivo_input)}")
        
        return caminho_csv

    except Exception as e:
        print(f"Erro crítico na extração: {e}")
        return None

if __name__ == "__main__":
    run()