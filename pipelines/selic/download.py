import os
import requests
import json
import time
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../data"))

def run():
    raw_dir = os.path.join(DATA_DIR, "raw/selic")
    os.makedirs(raw_dir, exist_ok=True)
    
    print("Baixando dados da Taxa Selic (Meta - Série 432) do Banco Central...")
    
    all_data = []
    ano_atual = datetime.now().year
    
    # Reduzimos o bloco para 5 anos. O BCB lida muito melhor com requisições menores.
    for ano_inicio in range(1999, ano_atual + 1, 5):
        ano_fim = min(ano_inicio + 4, ano_atual)
        
        if ano_inicio == 1999:
            data_inicial = "05/03/1999"
        else:
            data_inicial = f"01/01/{ano_inicio}"
            
        data_final = f"31/12/{ano_fim}"
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
        print(f"-> Extraindo bloco: {ano_inicio} a {ano_fim}...")
        
        sucesso_no_bloco = False
        
        for tentativa in range(1, 4):
            try:
                # Recriamos a sessão a cada tentativa para "limpar" nosso rastro e não sermos bloqueados
                session = requests.Session()
                session.headers.update({
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "application/json, text/plain, */*"
                })
                
                response = session.get(url, timeout=60)
                
                if response.status_code == 200:
                    texto_resposta = response.text.strip()
                    if not texto_resposta:
                        print(f"   [AVISO] BCB retornou vazio para o período {data_inicial} a {data_final}.")
                        sucesso_no_bloco = True
                        break
                    
                    try:
                        dados_parciais = response.json()
                        all_data.extend(dados_parciais)
                        sucesso_no_bloco = True
                        break # Sucesso! Sai do loop de tentativas
                    except json.JSONDecodeError:
                        print(f"   [ERRO] Tentativa {tentativa}/3: BCB enviou HTML em vez de JSON.")
                        print(f"   Trecho: {texto_resposta[:100]}...")
                        # Sem o break aqui, ele vai pausar e tentar de novo!
                else:
                    print(f"   [ERRO API] Status {response.status_code}. Tentativa {tentativa}/3...")
                    
            except requests.exceptions.Timeout:
                print(f"   [TIMEOUT] BCB demorou muito. Tentativa {tentativa}/3...")
            except requests.exceptions.RequestException as e:
                print(f"   [ERRO REDE] Conexão falhou. Tentativa {tentativa}/3...")
            
            # Se deu qualquer erro, espera 3 segundos antes de tentar o mesmo bloco
            time.sleep(3)

        if not sucesso_no_bloco:
            print(f"-> [FALHA CRÍTICA] Abortando após 3 tentativas no bloco {ano_inicio}-{ano_fim}.")
            return None
            
        # Pausa de 2 segundos entre um bloco e o próximo bloco de anos
        time.sleep(2)

    if not all_data:
        print("-> [FALHA] Nenhum dado foi extraído.")
        return None

    json_path = os.path.join(raw_dir, "selic.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(all_data, f)
        
    print(f"-> Download da Selic concluído com sucesso! Total de registros agrupados: {len(all_data)}")
    return json_path

if __name__ == "__main__":
    run()