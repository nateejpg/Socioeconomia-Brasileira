import pandas as pd
import requests
import os
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuração ---
EXTRACTED_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/extracted/bolsa_familia"
    )
)

def extract_bolsa_familia():
    print("--- 1. Extraction: Bolsa Familia Data (IPEA) ---")
    
    # URL da série VAL_PBF12
    url = "http://www.ipeadata.gov.br/api/odata4/Metadados('VAL_PBF12')/Valores"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json'
    }
    
    try:
        print(f"Acessando: {url}")
        
        response = requests.get(url, headers=headers, verify=False, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        
        if 'value' not in data or len(data['value']) == 0:
            print("Erro: Lista vazia.")
            return

        df = pd.DataFrame(data['value'])
        
        # MUDANÇA: Vamos renomear e manter as colunas de Território
        # TERCODIGO = 1 (Geralmente é Brasil)
        # TERNOME = Nome do lugar (Brasil, Acre, etc)
        rename_map = {
            'VALDATA': 'data', 
            'VALVALOR': 'valor',
            'TERCODIGO': 'ter_cod',
            'TERNOME': 'ter_nome',
            'NIVNOME': 'nivel' # País, Estado, Município
        }
        
        # Renomeia apenas o que encontrar
        df.rename(columns=rename_map, inplace=True)
        
        # Salva
        os.makedirs(EXTRACTED_DIR, exist_ok=True)
        output_path = os.path.join(EXTRACTED_DIR, 'bolsa_familia_ipea_raw.csv')
        
        df.to_csv(output_path, index=False)
        
        print(f"Sucesso: {len(df)} registros baixados.")
        print("Amostra (verifique a coluna 'ter_nome' ou 'nivel'):")
        print(df.head())
        
    except Exception as e:
        print(f"Erro fatal na extração: {e}")

if __name__ == "__main__":
    extract_bolsa_familia()