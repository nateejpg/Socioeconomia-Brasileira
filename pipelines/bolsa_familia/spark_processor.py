import os
import unicodedata
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, to_date, concat, lit, substring, regexp_replace

PROGRAMA = "bolsa_familia"

BRONZE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), f"../../data/bronze/{PROGRAMA}"))
SILVER_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), f"../../data/silver/{PROGRAMA}"))
GOLD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/gold/fato_bolsa_familia"))

def normalize_str(text):
    if not text: return ""
    return "".join(c for c in unicodedata.normalize('NFD', text) 
                   if unicodedata.category(c) != 'Mn').upper().strip()

def processar_com_spark(caminho_csv):
    spark = SparkSession.builder \
        .appName(f"{PROGRAMA}_Medallion") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        df_raw = spark.read.option("header", "true") \
            .option("delimiter", ";") \
            .option("encoding", "ISO-8859-1") \
            .csv(caminho_csv)
        
        # COMENTADO PARA ECONOMIZAR DISCO
        # df_raw.write.mode("append").parquet(BRONZE_DIR)

        cols_originais = df_raw.columns
        cols_normalizadas = [normalize_str(c) for c in cols_originais]
        df_silver = df_raw.toDF(*cols_normalizadas)

        mapa = {}
        found_targets = set()
        
        for c in df_silver.columns:
            target = None
            if "SIAFI" in c: target = "cod_municipio_siafi"
            elif "UF" in c and "NOME" not in c: target = "uf"
            elif "VALOR" in c and ("PARCELA" in c or "BENEFICIO" in c or "PAGO" in c): target = "valor"
            elif "NIS" in c: target = "nis"
            elif "REFER" in c or "COMPET" in c: target = "mes_ref"
            elif "MUNIC" in c and "NOME" in c: target = "nome_municipio"
            
            if not target and "COD" in c and "MUNIC" in c and "NOME" not in c:
                target = "cod_municipio_siafi"

            if target and target not in found_targets:
                mapa[c] = target
                found_targets.add(target)

        cols_existentes = [col(k).alias(v) for k, v in mapa.items()]
        df_silver = df_silver.select(cols_existentes)
        
        if "valor" in found_targets:
            df_silver = df_silver.withColumn("valor", regexp_replace("valor", ",", ".").cast("double"))

        # COMENTADO PARA ECONOMIZAR DISCO
        # df_silver.write.mode("append").parquet(SILVER_DIR)

        colunas_agrupamento = ["uf", "mes_ref"]
        if "cod_municipio_siafi" in found_targets: colunas_agrupamento.append("cod_municipio_siafi")
        if "nome_municipio" in found_targets: colunas_agrupamento.append("nome_municipio")

        df_gold = df_silver.groupBy(*colunas_agrupamento) \
            .agg(
                sum("valor").alias("valor_pago"),
                count("nis").alias("quantidade_beneficiarios")
            )

        df_gold = df_gold.withColumn(
            "referencia_data",
            to_date(concat(substring(col("mes_ref"), 1, 4), lit("-"), 
                           substring(col("mes_ref"), 5, 2), lit("-01")))
        ).drop("mes_ref")

        df_gold.write.mode("append").parquet(GOLD_DIR)

    except Exception as e:
        print(f"Erro Spark: {e}")
    finally:
        spark.stop()