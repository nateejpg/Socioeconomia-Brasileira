import os
import requests

RAW_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/raw/forca_trabalho"))

def run():
    os.makedirs(RAW_DIR, exist_ok=True)
    
    # URL corrigida com o código 32447 (Fora da Força) no lugar de 32450
    url = "https://apisidra.ibge.gov.br/values/t/6318/n1/all/v/1641/p/all/c629/32387,32446,32447"
    file_path = os.path.join(RAW_DIR, "ibge_forca_trabalho.json")
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers, timeout=180)
    
    if response.status_code == 200:
        with open(file_path, "wb") as f:
            f.write(response.content)
    else:
        raise ValueError(f"Falha ao realizar o download da API do IBGE. Status: {response.status_code}")
        
    return RAW_DIR