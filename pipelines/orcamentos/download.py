import os
import requests
from datetime import datetime

RAW_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/raw/orcamentos"))

def run():
    os.makedirs(RAW_DIR, exist_ok=True)
    
    ano_atual = datetime.now().year
    anos = list(range(2014, ano_atual + 1))
    
    url_base = "https://portaldatransparencia.gov.br/download-de-dados/orcamento-despesa"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    arquivos_baixados = 0
    
    for ano in anos:
        file_url = f"{url_base}/{ano}"
        file_path = os.path.join(RAW_DIR, f"orcamento_{ano}.zip")
        
        try:
            response = requests.get(file_url, headers=headers, timeout=180)
            if response.status_code == 200:
                with open(file_path, "wb") as f:
                    f.write(response.content)
                arquivos_baixados += 1
        except Exception as e:
            print(f"Falha no ano {ano}: {e}")
            
    if arquivos_baixados == 0:
        raise ValueError("Nenhum arquivo foi baixado.")
        
    return RAW_DIR