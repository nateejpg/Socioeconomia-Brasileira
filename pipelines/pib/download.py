import os
import requests
import re
from bs4 import BeautifulSoup

# Mudamos a URL para a RAIZ da pasta do PIB
ROOT_URL = "https://ftp.ibge.gov.br/Pib_Municipios/"
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/raw/pib"))

def run():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    try:
        # 1. LER A RAIZ DO IBGE E ACHAR A PASTA MAIS RECENTE
        print("Buscando a pasta mais recente no servidor do IBGE...")
        response = requests.get(ROOT_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        pastas_anos = []
        for link in soup.find_all('a'):
            href = link.get('href')
            # Exige que a pasta COMECE com um ano (ex: "2021/", "2022_2023/")
            if re.match(r'^20[1-9][0-9]', href) and href.endswith('/'):
                pastas_anos.append(href)
        
        if not pastas_anos:
            print("Nenhuma pasta de anos encontrada no IBGE.")
            return
            
        # Ordena a lista (a última será sempre a mais nova, ex: "2022_2023/")
        pastas_anos.sort()
        pasta_mais_recente = pastas_anos[-1]
        print(f"-> Pasta identificada: {pasta_mais_recente}")
        
        # 2. ACESSAR A SUBPASTA /base/ DENTRO DA PASTA MAIS RECENTE
        base_url = ROOT_URL + pasta_mais_recente + "base/"
        
        resp_base = requests.get(base_url)
        resp_base.raise_for_status()
        soup_base = BeautifulSoup(resp_base.text, 'html.parser')
        
        target_file = None
        for link in soup_base.find_all('a'):
            href = link.get('href')
            # Busca o arquivo da série nova (2010 até o ano atual) em XLSX
            if href.endswith('.zip') and '2010_' in href and 'xlsx' in href:
                target_file = href
                break
        
        if not target_file:
            print("Arquivo alvo (.xlsx da série 2010+) não encontrado.")
            return

        file_url = base_url + target_file
        file_path = os.path.join(OUTPUT_DIR, target_file)

        # 3. BAIXAR O ARQUIVO
        if os.path.exists(file_path):
            print(f"O arquivo mais atual já existe localmente: {target_file}")
            return

        print(f"Baixando nova base do PIB: {target_file}")
        r = requests.get(file_url, stream=True)
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print("-> Download concluído com sucesso!")

    except Exception as e:
        print(f"Erro no download do PIB: {e}")

if __name__ == "__main__":
    run()