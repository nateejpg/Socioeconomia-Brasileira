import os
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://ftp.ibge.gov.br/Pib_Municipios/2022_2023/base/"
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/raw/pib"))

def run():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    try:
        response = requests.get(BASE_URL)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        target_file = None
        
        for link in soup.find_all('a'):
            href = link.get('href')
            # Filtra apenas a SERIE NOVA (2010+) e formato XLSX (mais estavel)
            if href.endswith('.zip') and '2010_2023' in href and 'xlsx' in href:
                target_file = href
                break
        
        if not target_file:
            print("Arquivo alvo (2010-2023 .xlsx) não encontrado.")
            return

        file_url = BASE_URL + target_file
        file_path = os.path.join(OUTPUT_DIR, target_file)

        if os.path.exists(file_path):
            print(f"Arquivo já existe: {target_file}")
            return

        print(f"Baixando: {target_file}")
        r = requests.get(file_url, stream=True)
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print("Download concluído.")

    except Exception as e:
        print(f"Erro download: {e}")

if __name__ == "__main__":
    run()