import requests
import time
import os

# Pasta onde os CSVs serão salvos
RAW_FOLDER = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/raw/receita"
    )
)

os.makedirs(RAW_FOLDER, exist_ok=True)

# Headers para evitar bloqueio básico do portal
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/csv,application/octet-stream",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"
}

# Ano inicial e final
start_year = 2013
end_year = 2026

for year in range(start_year, end_year + 1):

    file_path = os.path.join(RAW_FOLDER, f"receita_{year}.zip")

    url = f"https://portaldatransparencia.gov.br/download-de-dados/receitas/{year}"
    print(f"Baixando dados para {year} …")

    try:
        response = requests.get(url, headers=HEADERS, timeout=180)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao baixar {year}: {e}")
        continue

    # Criar arquivo local
    with open(file_path, "wb") as f:
        f.write(response.content)

    print(f"Baixado e salvo: {file_path}")

    # Pausa entre requisições (evita bloqueio)
    time.sleep(2)

print("Download concluído!")
