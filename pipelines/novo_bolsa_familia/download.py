import requests
import time
import os

RAW_FOLDER = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/raw/novo_bolsa_familia"
    )
)

os.makedirs(RAW_FOLDER, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
}

def download_data(ano, mes):
    mes_str = f"{mes:02d}"
    file_name = f"novo_bolsa_familia_{ano}{mes_str}.zip"
    file_path = os.path.join(RAW_FOLDER, file_name)
    
    url = f"https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia/{ano}{mes_str}"
    
    if os.path.exists(file_path):
        return file_path

    try:
        print(f"⬇ [Novo Bolsa] Baixando {ano}/{mes_str}...")
        response = requests.get(url, headers=HEADERS, stream=True, timeout=180)
        
        if response.status_code == 200 and 'text/html' not in response.headers.get('Content-Type', ''):
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)
            return file_path
        else:
            print(f" Arquivo indisponível: {ano}/{mes_str}")
            return None

    except requests.exceptions.RequestException as e:
        print(f" Erro de conexão: {e}")
        if os.path.exists(file_path): os.remove(file_path)
        return None