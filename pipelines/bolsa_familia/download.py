import requests
import os
import time

def download_zip(ano, mes):

    base_url = "https://portaldatransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos"
    referer_url = "https://portaldatransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos"
    
    download_url = f"{base_url}/{ano}{mes}"
    
    output_dir = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), 
            "../../data/raw/bolsa_familia"
            ))
    
    os.makedirs(output_dir, exist_ok=True)
    
    zip_path = os.path.join(output_dir, f"bolsa_familia_{ano}_{mes}.zip")

    if os.path.exists(zip_path):
        print(f"[PULANDO] {mes}/{ano}: Arquivo já existe.")
        return zip_path

    print(f"⬇[BAIXANDO] {mes}/{ano}...")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Referer": referer_url,
        "Upgrade-Insecure-Requests": "1"
    }

    session = requests.Session()
    session.headers.update(headers)

    try:
        session.get(referer_url, timeout=10)
        
        response = session.get(download_url, stream=True, timeout=120)
        response.raise_for_status()

        if 'text/html' in response.headers.get('Content-Type', ''):
            print(f"[AVISO] {mes}/{ano}: Servidor retornou HTML (possível mudança de nome do programa ou erro).")
            return None

        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)
                    
        print(f"[SUCESSO] {mes}/{ano} salvo!")
        return zip_path

    except Exception as e:
        print(f"[ERRO] {mes}/{ano}: {e}")
        if os.path.exists(zip_path): os.remove(zip_path)
        return None

if __name__ == "__main__":

    print("Iniciando Download em Lote (2013 a 2021)...")
    
    anos = range(2013, 2022)
    meses = range(1, 13)

    for ano in anos:
        for mes in meses:
            mes_str = f"{mes:02d}"
            
            download_zip(str(ano), mes_str)
            
            time.sleep(1)
            
    print("\nProcesso de download finalizado!")