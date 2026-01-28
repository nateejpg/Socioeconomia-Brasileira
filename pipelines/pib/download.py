import pandas as pd
import sidrapy
import os

# --- Configuração de Caminhos ---
EXTRACTED_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../data/extracted/pib"
    )
)

def extract_gdp():
    print("--- 1. Extraction: GDP Data (IBGE SIDRA) ---")
    
    try:
        # MUDANÇA TOTAL DE ESTRATÉGIA:
        # Tabela 1846: PIB a preços de mercado (Valores correntes)
        # Variável 585: Valor a preços correntes (Milhões de Reais)
        # Classificação 11255 = 90707: Filtra apenas o "PIB Total" (evita baixar Agro/Indústria separado)
        gdp_raw = sidrapy.get_table(
            table_code="1846", 
            territorial_level="1",
            ibge_territorial_code="1", 
            variable="585", 
            classifications={"11255": "90707"}, # 90707 = PIB a preços de mercado
            period="all"
        )
        
        # O Sidra retorna a primeira linha como cabeçalho
        gdp_df = gdp_raw.iloc[1:].copy()
        gdp_df.columns = gdp_raw.iloc[0]
        
        os.makedirs(EXTRACTED_DIR, exist_ok=True)
        
        output_path = os.path.join(EXTRACTED_DIR, 'pib_ibge_raw.csv')
        
        gdp_df.to_csv(output_path, index=False)
        
        print(f"Sucesso: Dados do PIB (R$) salvos em: {output_path}")
        print(gdp_df.head())
        
    except Exception as e:
        print(f"Erro durante a extracao: {e}")

if __name__ == "__main__":
    extract_gdp()