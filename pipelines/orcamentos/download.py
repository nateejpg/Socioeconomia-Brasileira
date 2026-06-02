import os
import requests
import time
from datetime import datetime

RAW_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/raw/orcamentos"))

def run():
    os.makedirs(RAW_DIR, exist_ok=True)
    
    ano_atual = datetime.now().year
    anos = list(range(2014, ano_atual + 1))
    
    url_base = "https://portaldatransparencia.gov.br/download-de-dados/orcamento-despesa"
    
    # Cabeçalhos robustos para imitar perfeitamente um navegador real
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1'
    }
    
    arquivos_baixados = 0
    
    # Criar uma sessão para manter os cookies vivos e convencer o Firewall
    session = requests.Session()
    session.headers.update(headers)
    
    for ano in anos:
        file_url = f"{url_base}/{ano}"
        file_path = os.path.join(RAW_DIR, f"orcamento_{ano}.zip")
        print(f"Tentando baixar Orçamento de {ano}...")
        
        try:
            # O stream=True salva o arquivo em partes, impedindo que o GitHub Actions fique sem memória
            response = session.get(file_url, timeout=180, stream=True)
            
            if response.status_code == 200:
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                arquivos_baixados += 1
                print(f"-> Sucesso: Orçamento {ano} baixado e salvo.")
            else:
                print(f"-> Bloqueio/Erro no {ano}: Status Code {response.status_code}")
                
        except Exception as e:
            print(f"-> Falha de conexão no ano {ano}: {e}")
            
        # Pausa de 3 segundos entre downloads para não ser bloqueado por taxa de requisições (Rate Limit)
        time.sleep(3)
            
    if arquivos_baixados == 0:
        raise ValueError("Nenhum arquivo foi baixado.")
        
    return RAW_DIR