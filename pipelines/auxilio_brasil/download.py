import requests
import time
import os

RAW_FOLDER = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/raw/auxilio_brasil"
    )
)

os.makedirs(RAW_FOLDER, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
}

years = [2021, 2022, 2023]
months = range(1, 13)

print(f"Iniciando download em: {RAW_FOLDER}")

for year in years:
    for month in months:
        mes_str = f"{month:02d}"
        file_name = f"auxilio_brasil_{year}{mes_str}.zip"
        file_path = os.path.join(RAW_FOLDER, file_name)
        
        # URL específica do Auxílio Brasil
        url = f"https://portaldatransparencia.gov.br/download-de-dados/auxilio-brasil/{year}{mes_str}"
        
        if os.path.exists(file_path):
            continue

        try:
            print(f"Baixando {year}/{mes_str}...")
            response = requests.get(url, headers=HEADERS, stream=True, timeout=180)
            
            # O portal retorna HTML (200 OK) quando o arquivo não existe (erro comum em meses futuros ou passados fora da vigência)
            if response.status_code == 200 and 'text/html' not in response.headers.get('Content-Type', ''):
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1024*1024):
                        if chunk:
                            f.write(chunk)
                print(f"Salvo: {file_name}")
            else:
                print(f"Arquivo não disponível para {year}/{mes_str} (Provável mudança de programa).")
            
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão: {e}")