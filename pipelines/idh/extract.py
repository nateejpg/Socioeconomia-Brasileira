import pandas as pd
import os
import io
import re

EXTRACTED_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/extracted/idh"
    )
)

def run(html_path):
    if not os.path.exists(EXTRACTED_DIR):
        os.makedirs(EXTRACTED_DIR, exist_ok=True)
    
    caminho_dest = os.path.join(EXTRACTED_DIR, "idh_extracted.csv")
    print("Procurando a tabela histórica de Evolução dos Estados...")
    
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # O decimal=',' garante que 0,814 vire 0.814 matemático
    tabelas = pd.read_html(io.StringIO(html_content), decimal=',', thousands='.')
    
    df_alvo = None
    for tb in tabelas:
        # Procuramos especificamente uma tabela com cabeçalhos múltiplos (MultiIndex)
        # que contenha o termo 'Unidade Federativa' e histórico (ex: 2010)
        if isinstance(tb.columns, pd.MultiIndex):
            colunas_texto = " ".join([str(c) for c in tb.columns.values])
            if 'Unidade Federativa' in colunas_texto and '2010' in colunas_texto:
                df_alvo = tb
                break
                
    if df_alvo is None:
        raise ValueError("A tabela 'Evolução dos estados' não foi encontrada no HTML.")

    col_estado = [c for c in df_alvo.columns if 'Unidade Federativa' in str(c)][0]
    
    dados_longos = []
    
    for col in df_alvo.columns:
        niveis_coluna = " ".join(str(c) for c in col)
        
        ano_match = re.search(r'\b(19\d{2}|20\d{2})\b', niveis_coluna)
        
        if ano_match:
            ano = int(ano_match.group(1))
            metrica_bruta = str(col[-1]).strip().upper()
            
            if metrica_bruta == 'IDH': col_name = 'idhm'
            elif metrica_bruta == 'E': col_name = 'idhm_e'
            elif metrica_bruta == 'R': col_name = 'idhm_r'
            elif metrica_bruta == 'L': col_name = 'idhm_l'
            else: continue
                
            for idx, row in df_alvo.iterrows():
                estado_nome = str(row[col_estado]).replace('\xa0', ' ').strip()
                valor = row[col]
                
                if pd.notna(valor) and str(valor).replace('.', '', 1).isdigit():
                    dados_longos.append({
                        'estado_limpo': estado_nome,
                        'ano': ano,
                        'metrica': col_name,
                        'valor': float(valor)
                    })
                
    df_long = pd.DataFrame(dados_longos)
    
    df_final = df_long.pivot_table(
        index=['estado_limpo', 'ano'], 
        columns='metrica', 
        values='valor', 
        aggfunc='first'
    ).reset_index()
    
    total_linhas = len(df_final)
    print(f"-> Tabela histórica extraída! Total de registros: {total_linhas} (Estados x Anos).")
    
    df_final.to_csv(caminho_dest, index=False)
    
    return caminho_dest