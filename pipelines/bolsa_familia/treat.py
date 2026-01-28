import pandas as pd
import os

EXTRACTED_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/extracted/bolsa_familia"
    )
)

PROCESSED_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/processed/bolsa_familia"
    )
)

os.makedirs(PROCESSED_DIR, exist_ok=True)

def treat_bolsa_familia():
    print("--- 2. Transformation: Limpando Bolsa Família ---")
    
    input_path = os.path.join(EXTRACTED_DIR, "bolsa_familia_ipea_raw.csv")
    output_path = os.path.join(PROCESSED_DIR, "bolsa_familia_clean.csv")
    
    if not os.path.exists(input_path):
        print(f"Erro: Arquivo bruto não encontrado em {input_path}")
        return

    try:
        df = pd.read_csv(input_path)
        
        # 1. Filtrar Apenas BRASIL
        if 'ter_cod' in df.columns:
            # Garante que ter_cod seja numérico para evitar erros de comparação
            df['ter_cod'] = pd.to_numeric(df['ter_cod'], errors='coerce')
            df = df[df['ter_cod'] == 0].copy()
            print("Filtro aplicado: Apenas dados nacionais (ter_cod=0).")
        elif 'nivel' in df.columns:
            df = df[df['nivel'] == 'Brasil'].copy()
        
        # 2. Converter Data (CORREÇÃO AQUI)
        # utc=True força o pandas a ignorar a bagunça de fusos horários e padronizar tudo
        df['data'] = pd.to_datetime(df['data'], utc=True)
        
        df['ano'] = df['data'].dt.year
        df['mes'] = df['data'].dt.month
        
        # 3. Converter e Corrigir Valor
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        # Ajuste de escala: Milhares -> Reais
        df['valor'] = df['valor'] * 1000 
        
        # 4. Limpeza final
        df.dropna(subset=['valor'], inplace=True)
        df = df[df['ano'] >= 2004]
        
        # 5. Formatar para o Banco
        df_final = df[['ano', 'mes', 'valor']].sort_values(['ano', 'mes'])
        df_final.rename(columns={'valor': 'valor_pago_reais'}, inplace=True)
        
        df_final.to_csv(output_path, index=False)
        
        print(f"Sucesso: {len(df_final)} meses consolidados.")
        print("--- Amostra Recente ---")
        print(df_final.tail())

    except Exception as e:
        print(f"Erro no tratamento: {e}")

if __name__ == "__main__":
    treat_bolsa_familia()