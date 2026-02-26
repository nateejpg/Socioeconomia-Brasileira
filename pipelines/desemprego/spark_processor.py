import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../data"))

def run(csv_path):
    spark = SparkSession.builder.appName("Desemprego_Medallion").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("[Spark] Lendo arquivo extraído...")
    df_raw = spark.read.option("header", "true").csv(csv_path)

    bronze_path = os.path.join(DATA_DIR, "bronze/desemprego")
    df_raw.write.mode("overwrite").parquet(bronze_path)

    print("[Spark] Processando Camada Silver...")
    
    # 1. FAXINA BLINDADA: Filtra apenas linhas que são dados reais (Ignora rodapés do IBGE)
    # Exige que o trimestre tenha tamanho 6 (ex: "202301") e o estado seja numérico (ex: "35")
    df_silver = df_raw.filter(
        F.col("trimestre_cod").isNotNull() & 
        F.col("cod_uf").isNotNull() &
        (F.length(F.trim(F.col("trimestre_cod"))) == 6) &
        (F.col("cod_uf").rlike("^[0-9]+$"))
    )

# 2. Tratamento da taxa e do estado (Convertendo vírgula do IBGE para ponto matemático)
    df_silver = df_silver.withColumn("taxa_desemprego", F.trim(F.regexp_replace(F.col("taxa_desemprego"), ",", "."))) \
                         .withColumn("taxa_desemprego", 
                                  F.when(F.col("taxa_desemprego").rlike(r"^[0-9]+(\.[0-9]+)?$"), 
                                         F.col("taxa_desemprego").cast("double"))
                                  .otherwise(None)) \
                      .withColumn("cod_uf", F.col("cod_uf").cast("int"))

    # 3. Quebrando "202301" em Ano (2023) e Trimestre (1) - Agora garantido que só tem números!
    df_silver = df_silver.withColumn("ano", F.substring(F.col("trimestre_cod"), 1, 4).cast("int")) \
                         .withColumn("trimestre", F.substring(F.col("trimestre_cod"), 5, 2).cast("int"))
    
    silver_path = os.path.join(DATA_DIR, "silver/desemprego")
    df_silver.write.mode("overwrite").parquet(silver_path)

    print("[Spark] Processando Camada Gold...")
    df_gold = df_silver.filter(F.col("taxa_desemprego").isNotNull()) \
                       .select("ano", "trimestre", "cod_uf", "uf_nome", "taxa_desemprego")
    
    gold_path = os.path.join(DATA_DIR, "gold/desemprego")
    df_gold.write.mode("overwrite").parquet(gold_path)
    
    print(f"-> Sucesso no Spark! Total de registros salvos na Gold: {df_gold.count()}")

if __name__ == "__main__":
    pass