import os
import json
import csv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../data"))

def run():
    json_path = os.path.join(DATA_DIR, "raw/selic/selic.json")
    extracted_dir = os.path.join(DATA_DIR, "extracted/selic")
    os.makedirs(extracted_dir, exist_ok=True)
    
    csv_path = os.path.join(extracted_dir, "selic.csv")
    print("Extraindo e convertendo JSON da Selic para CSV...")
    
    if not os.path.exists(json_path):
        print("Arquivo JSON não encontrado.")
        return None

    with open(json_path, 'r', encoding='utf-8') as f:
        dados = json.load(f)
        
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["data", "valor"])
        writer.writeheader()
        writer.writerows(dados)
        
    print(f"-> CSV extraído com sucesso! Total de registos: {len(dados)}")
    return csv_path

if __name__ == "__main__":
    run()