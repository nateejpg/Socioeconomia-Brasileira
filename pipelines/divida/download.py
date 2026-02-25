import os
import requests
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../data"))

def run():
    raw_dir = os.path.join(DATA_DIR, "raw/divida")
    os.makedirs(raw_dir, exist_ok=True)
    
    # Série 13761: Dívida Bruta do Governo Geral (Saldo em R$ Milhões)
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.13761/dados?formato=json"
    print(f"Baixando dados da Dívida (Série 13761) da API do Banco Central...")
    
    # TRUQUE: Disfarçar o script como um navegador para o BCB não bloquear
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    # Passamos os headers na requisição
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        json_path = os.path.join(raw_dir, "divida_bruta.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(response.json(), f)
        print("-> Download da Dívida em R$ Milhões realizado com sucesso!")
        return json_path
    else:
        print(f"-> Erro na API do BCB: {response.status_code}")
        print(response.text) # Para vermos o erro exato se falhar
        return None

if __name__ == "__main__":
    run()