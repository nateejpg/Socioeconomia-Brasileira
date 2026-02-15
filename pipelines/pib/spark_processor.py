import os
import sys
import glob
import unicodedata
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim

BASE_DIR = os.path.dirname(__file__)
EXTRACTED_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../data/extracted/pib"))
BRONZE_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../data/bronze/pib"))
SILVER_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../data/silver/pib"))
GOLD_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../data/gold/pib"))

def normalize_col_name(text):
    """Limpa nomes de colunas: remove acentos, parenteses e quebras de linha."""
    if not text: return "coluna_desconhecida"
    
    text = text.replace("\n", " ").replace("\r", " ")
    
    text = re.sub(r"\(.*?\)", "", text) 
    
    text = "".join(c for c in unicodedata.normalize('NFD', text) 
                   if unicodedata.category(c) != 'Mn')
    
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "_", text) 
    text = text.strip("_") 
    
    return text

def run(caminho_csv):
    if not os.path.exists(caminho_csv):
        print(f"Erro: Arquivo CSV não encontrado: {caminho_csv}")
        return

    print(f"Iniciando processamento Spark: {caminho_csv}")

    spark = SparkSession.builder \
        .appName("PIB_Medallion") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:

        print("--> Gerando camada BRONZE...")
        
        df_raw = spark.read.option("header", "true") \
            .option("delimiter", ";") \
            .option("encoding", "UTF-8") \
            .option("multiLine", "true") \
            .option("inferSchema", "false") \
            .option("quote", "\"") \
            .option("escape", "\"") \
            .csv(caminho_csv)

        df_raw.write.mode("overwrite").parquet(BRONZE_DIR)
        
        print("--> Gerando camada SILVER...")
        
        cols_originais = df_raw.columns
        cols_novas = []
        
        for c in cols_originais:
            novo_nome = normalize_col_name(c)
            if novo_nome in cols_novas:
                contador = 2
                temp_nome = f"{novo_nome}_{contador}"
                while temp_nome in cols_novas:
                    contador += 1
                    temp_nome = f"{novo_nome}_{contador}"
                novo_nome = temp_nome
            cols_novas.append(novo_nome)

        df_silver = df_raw.toDF(*cols_novas)

        if "ano" in df_silver.columns:
             df_silver = df_silver.filter(col("ano").rlike("^[0-9]+$"))

        cols_valor = [
            c for c in df_silver.columns 
            if any(x in c for x in ["valor_adicionado", "impostos", "produto_interno", "pib"])
            and "atividade" not in c 
        ]

        for c in cols_valor:
            df_silver = df_silver.withColumn(
                c, 
                regexp_replace(col(c), ",", ".").cast("double")
            )

        if "ano" in df_silver.columns:
            df_silver = df_silver.withColumn("ano", col("ano").cast("int"))

        col_cod_mun = [c for c in df_silver.columns if "codigo_do_municipio" in c]
        if col_cod_mun:
             df_silver = df_silver.withColumn(col_cod_mun[0], col(col_cod_mun[0]).cast("string"))

        df_silver.write.mode("overwrite").parquet(SILVER_DIR)

        print("--> Gerando camada GOLD...")
        
        mapa_colunas = {
            "ano": "ano",
            "codigo_da_grande_regiao": "cod_regiao",
            "nome_da_grande_regiao": "regiao",
            "sigla_da_unidade_da_federacao": "uf",
            "codigo_do_municipio": "cod_municipio_ibge",
            "nome_do_municipio": "nome_municipio",
            "valor_adicionado_bruto_da_agropecuaria": "vab_agro",
            "valor_adicionado_bruto_da_industria": "vab_industria",
            "valor_adicionado_bruto_dos_servicos": "vab_servicos",
            "valor_adicionado_bruto_da_administracao": "vab_publico",
            "impostos_liquidos": "impostos",
            "produto_interno_bruto_a_precos": "pib", 
            "produto_interno_bruto_per_capita": "pib_per_capita"
        }

        cols_select = []
        df_cols_silver = df_silver.columns

        for trecho_silver, nome_gold in mapa_colunas.items():
            matches = [c for c in df_cols_silver if trecho_silver in c]
            
            if matches:
                best_match = matches[0]
                # Evita confundir PIB Total com Per Capita
                if nome_gold == "pib" and "per_capita" in best_match:
                    if len(matches) > 1:
                        best_match = matches[1]
                
                if nome_gold == "pib" and "per_capita" in best_match:
                    continue

                cols_select.append(col(best_match).alias(nome_gold))

        df_gold = df_silver.select(*cols_select)
        df_gold.write.mode("overwrite").parquet(GOLD_DIR)
        
        print("Sucesso! Pipeline PIB finalizada com sucesso.")

    except Exception as e:
        print(f"ERRO FATAL NO SPARK: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        run(sys.argv[1])
    else:
        # Modo automático
        csvs = glob.glob(os.path.join(EXTRACTED_DIR, "*.csv"))
        if csvs:
            run(csvs[0])
        else:
            print(f"Nenhum CSV encontrado em {EXTRACTED_DIR}")