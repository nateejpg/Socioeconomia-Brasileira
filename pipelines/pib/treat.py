import pandas as pd
import os
import numpy as np

# Configuracao de Caminhos
EXTRACTED_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/extracted/pib"
    )
)

PROCESSED_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/processed/pib"
    )
)

os.makedirs(PROCESSED_DIR, exist_ok=True)

def treat_pib():
    print("--- 2. Transformation: Limpando dados do PIB ---")
    
    input_path = os.path.join(EXTRACTED_DIR, "pib_ibge_raw.csv")
    output_path = os.path.join(PROCESSED_DIR, "pib_clean.csv")
    
    if not os.path.exists(input_path):
        print(f"[ERROR] Arquivo bruto nao encontrado em {input_path}")
        return

    try:
        df = pd.read_csv(input_path)
        
        # Validacao de colunas
        if 'Valor' not in df.columns or 'Trimestre (Código)' not in df.columns:
            print(f"[ERROR] Colunas esperadas ('Valor', 'Trimestre (Código)') nao encontradas.")
            print(f"Colunas disponiveis: {df.columns.tolist()}")
            return

        # Selecao e Renomeacao
        df = df[['Trimestre (Código)', 'Valor']].copy()
        
        df.rename(columns={
            'Trimestre (Código)': 'ano_trimestre_cod',
            'Valor': 'valor_pib_milhoes'
        }, inplace=True)
        
        # --- Limpeza de Valores ---
        # 1. Converter para string
        df['valor_pib_milhoes'] = df['valor_pib_milhoes'].astype(str)
        
        # 2. Substituir virgula por ponto (caso exista decimal)
        df['valor_pib_milhoes'] = df['valor_pib_milhoes'].str.replace(',', '.')
        
        # 3. Remover tracos ou reticencias
        df['valor_pib_milhoes'] = df['valor_pib_milhoes'].replace(['...', '..', '-', 'nan'], np.nan)
        
        # 4. Converter para float
        df['valor_pib_milhoes'] = pd.to_numeric(df['valor_pib_milhoes'], errors='coerce')
        
        # 5. Remover linhas vazias
        df.dropna(subset=['valor_pib_milhoes'], inplace=True)
        
        # --- Tratamento de Data ---
        # Formato esperado: 199601 (Ano + Numero do Trimestre)
        df['ano_trimestre_cod'] = df['ano_trimestre_cod'].astype(str)
        
        # 4 primeiros digitos sao o Ano
        df['ano'] = df['ano_trimestre_cod'].str[:4].astype(int)
        
        # O ultimo digito e o Trimestre
        df['trimestre'] = df['ano_trimestre_cod'].str[-1].astype(int)
        
        # Ordenacao
        df_final = df[['ano', 'trimestre', 'valor_pib_milhoes']].sort_values(['ano', 'trimestre'])
        
        # Salvar
        df_final.to_csv(output_path, index=False)
        
        print(f"[SUCCESS] {len(df_final)} linhas salvas em {output_path}")
        print(df_final.head())

    except Exception as e:
        print(f"[ERROR] Erro durante a transformacao: {e}")

if __name__ == "__main__":
    treat_pib()