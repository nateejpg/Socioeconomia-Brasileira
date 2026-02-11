import requests
import os
import time

RAW_FOLDER = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/raw/receita"
    )
)

os.makedirs(RAW_FOLDER, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/csv,application/octet-stream"
}

def download_receita(ano):
    file_name = f"receita_{ano}.zip"
    file_path = os.path.join(RAW_FOLDER, file_name)
    
    # URL oficial (Verifique se o padrao do ano se mantem)
    url = f"https://portaldatransparencia.gov.br/download-de-dados/receitas/{ano}"
    
    if os.path.exists(file_path):
        return file_path

    print(f"Baixando Receita {ano}...")
    
    try:
        response = requests.get(url, headers=HEADERS, stream=True, timeout=300)
        
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
            return file_path
        else:
            print(f"Erro download {ano}: Status {response.status_code}")
            return None

    except Exception as e:
        print(f"Erro conexao {ano}: {e}")
        if os.path.exists(file_path): os.remove(file_path)
        return None