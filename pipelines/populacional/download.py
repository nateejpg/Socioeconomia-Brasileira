import os
import requests

RAW_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/raw/populacao"))

def run():
    os.makedirs(RAW_DIR, exist_ok=True)
    
    url = "http://api.worldbank.org/v2/country/BR/indicator/SP.POP.TOTL?format=json&per_page=100"
    file_path = os.path.join(RAW_DIR, "populacao_br_raw.json")
    
    response = requests.get(url, timeout=180)
    
    if response.status_code == 200:
        with open(file_path, "wb") as f:
            f.write(response.content)
    else:
        raise ValueError("Falha ao realizar o download da API.")
        
    return RAW_DIR