import requests
import os

RAW_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/raw/idh"
    )
)

def run():
    if not os.path.exists(RAW_DIR):
        os.makedirs(RAW_DIR, exist_ok=True)
    
    # Agora salvamos como HTML bruto, não mais CSV
    arquivo_local = os.path.join(RAW_DIR, "idh_raw.html")
    print("Iniciando Download do HTML bruto da Wikipedia...")
    
    url = "https://pt.wikipedia.org/wiki/Lista_de_unidades_federativas_do_Brasil_por_IDH"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    response = requests.get(url, headers=headers)
    
    # FLAG/VERIFICAÇÃO: Verifica se baixou algo ou não
    if response.status_code == 200 and len(response.text) > 0:
        print(f"-> Download realizado com sucesso! Tamanho do arquivo: {len(response.text)} bytes.")
    else:
        raise ValueError(f"Falha no download. Status Code: {response.status_code}, Tamanho: {len(response.text)} bytes.")
    
    # Salva o texto bruto da internet
    with open(arquivo_local, 'w', encoding='utf-8') as f:
        f.write(response.text)
    
    return arquivo_local