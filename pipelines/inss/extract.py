import os
import shutil
import pandas as pd

EXTRACT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/extracted/inss"))

def run(raw_dir):
    print("[EXTRACT] A limpar os dados brutos...")
    if os.path.exists(EXTRACT_DIR):
        shutil.rmtree(EXTRACT_DIR)
    os.makedirs(EXTRACT_DIR, exist_ok=True)
    
    input_file = os.path.join(raw_dir, "macro_inss_historico.csv")
    output_file = os.path.join(EXTRACT_DIR, "inss_limpo.csv")
    
    if not os.path.exists(input_file):
        raise FileNotFoundError("O ficheiro bruto não foi encontrado!")
        
    df = pd.read_csv(input_file, sep=";")
    
    # Simula o processo de qualidade de dados (renomear colunas, remover nulos)
    df = df.rename(columns={
        "ano": "ano_competencia",
        "estado_uf": "uf",
        "total_emitido": "quantidade_aposentados"
    })
    df = df.dropna()
    
    df.to_csv(output_file, index=False, sep=";")
    print(f"[EXTRACT] Ficheiro limpo guardado em: {output_file}")
    
    return EXTRACT_DIR