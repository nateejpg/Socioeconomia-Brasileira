import os
import unicodedata
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, to_date, regexp_replace, year, month

PROGRAMA = "receita"

BRONZE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), f"../../data/bronze/{PROGRAMA}"))
SILVER_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), f"../../data/silver/{PROGRAMA}"))
GOLD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), f"../../data/gold/{PROGRAMA}"))

def normalize_col_name(text):
    """Remove acentos, espaços e deixa minusculo (Ex: 'ÓRGÃO SUPERIOR' -> 'orgao_superior')"""
    if not text: return ""
    return "".join(c for c in unicodedata.normalize('NFD', text) 
                   if unicodedata.category(c) != 'Mn').lower().replace(" ", "_")

def processar_receita_spark(caminho_csv):
    spark = SparkSession.builder \
        .appName("Receita_Medallion") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        df_raw = spark.read.option("header", "true") \
            .option("delimiter", ";") \
            .option("encoding", "ISO-8859-1") \
            .csv(caminho_csv)

        df_raw.write.mode("append").parquet(BRONZE_DIR)
        
        cols_novas = [normalize_col_name(c) for c in df_raw.columns]
        df_silver = df_raw.toDF(*cols_novas)

        cols_valor = [c for c in df_silver.columns if "valor" in c]
        
        for c in cols_valor:
            df_silver = df_silver.withColumn(
                c, 
                regexp_replace(
                    regexp_replace(col(c), "\\.", ""), 
                    ",", "." 
                ).cast("double")
            )

        if "data_lancamento" in df_silver.columns:
            df_silver = df_silver.withColumn(
                "data_lancamento", 
                to_date(col("data_lancamento"), "dd/MM/yyyy")
            )

        df_silver.write.mode("append").parquet(SILVER_DIR)

        if "data_lancamento" in df_silver.columns and "valor_realizado" in df_silver.columns:
            
            df_gold = df_silver.withColumn("ano", year("data_lancamento")) \
                               .withColumn("mes", month("data_lancamento"))
            
            cols_group = ["ano", "mes", "categoria_economica", "origem_receita"]
            
            cols_existentes = [c for c in cols_group if c in df_gold.columns]
            
            df_agg = df_gold.groupBy(*cols_existentes) \
                .agg(
                    sum("valor_realizado").alias("total_arrecadado"),
                    sum("valor_previsto_atualizado").alias("total_previsto"),
                    count("*").alias("qtd_lancamentos")
                )
            
            df_agg.write.mode("append").parquet(GOLD_DIR)

    except Exception as e:
        print(f"Erro Spark Receita: {e}")
    finally:
        spark.stop()