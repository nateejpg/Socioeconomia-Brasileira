import os
import requests
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../data"))

def run():
    raw_dir = os.path.join(DATA_DIR, "raw/desemprego")
    os.makedirs(raw_dir, exist_ok=True)
    
    # API IBGE SIDRA - Tabela 4099 (Taxa de Desocupação por UF - Trimestral)
    url = "https://apisidra.ibge.gov.br/values/t/4099/n3/all/v/4099/p/all"
    print("Baixando dados de Desemprego (Taxa de Desocupação) da API do IBGE...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        json_path = os.path.join(raw_dir, "desemprego_ibge.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(response.json(), f)
        print("-> Download do Desemprego (JSON) realizado com sucesso!")
        return json_path
    else:
        print(f"-> Erro na API do IBGE SIDRA: {response.status_code}")
        return None

if __name__ == "__main__":
    run()