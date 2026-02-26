import os
import json
import csv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../data"))

def run():
    json_path = os.path.join(DATA_DIR, "raw/desemprego/desemprego_ibge.json")
    extracted_dir = os.path.join(DATA_DIR, "extracted/desemprego")
    os.makedirs(extracted_dir, exist_ok=True)
    
    csv_path = os.path.join(extracted_dir, "desemprego_ibge.csv")
    print("Extraindo e convertendo JSON do SIDRA para CSV (Mapeamento Dinâmico)...")
    
    if not os.path.exists(json_path):
        print("Arquivo JSON não encontrado.")
        return None

    with open(json_path, 'r', encoding='utf-8') as f:
        dados_brutos = json.load(f)
        
    cabecalho = dados_brutos[0]
    dados_validos = dados_brutos[1:]
    
    # 1. MAPEAMENTO DINÂMICO DE CHAVES (Lê o JSON e acha a coluna certa)
    key_uf_cod = None
    key_uf_nome = None
    key_trimestre = None
    key_valor = None
    
    for k, v in cabecalho.items():
        v_str = str(v).lower()
        if "unidade da federação" in v_str and "código" in v_str:
            key_uf_cod = k
        elif "unidade da federação" in v_str:
            key_uf_nome = k
        elif "trimestre" in v_str and "código" in v_str:
            key_trimestre = k
        elif v_str == "valor":
            key_valor = k

    # Fallback de segurança
    if not key_uf_cod: key_uf_cod = "D1C" if "D1C" in cabecalho else "d1c"
    if not key_uf_nome: key_uf_nome = "D1N" if "D1N" in cabecalho else "d1n"
    if not key_trimestre: key_trimestre = "D2C" if "D2C" in cabecalho else "d2c"
    if not key_valor: key_valor = "V" if "V" in cabecalho else "v"

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["cod_uf", "uf_nome", "trimestre_cod", "taxa_desemprego"])
        writer.writeheader()
        
        for linha in dados_validos:
            writer.writerow({
                "cod_uf": linha.get(key_uf_cod, ""),
                "uf_nome": linha.get(key_uf_nome, ""),
                "trimestre_cod": linha.get(key_trimestre, ""),
                "taxa_desemprego": linha.get(key_valor, "")
            })
            
    print(f"-> CSV extraído com sucesso! Total de registros: {len(dados_validos)}")
    return csv_path

if __name__ == "__main__":
    run()