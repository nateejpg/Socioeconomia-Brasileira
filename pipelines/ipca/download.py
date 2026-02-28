import os
import requests
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../data"))

def run():
    raw_dir = os.path.join(DATA_DIR, "raw/ipca")
    os.makedirs(raw_dir, exist_ok=True)
    
    # Série 433: IPCA - Variação mensal (%)
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json"
    print("A transferir dados da Inflação (IPCA - Série 433) do Banco Central...")
    
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*"
    })
    
    try:
        response = session.get(url, timeout=30)
        
        if response.status_code == 200:
            json_path = os.path.join(raw_dir, "ipca.json")
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(response.json(), f)
            print("-> Download do IPCA concluído com sucesso!")
            return json_path
        else:
            print(f"-> Erro na API do BCB: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"-> Erro de ligação: {e}")
        return None

if __name__ == "__main__":
    run()