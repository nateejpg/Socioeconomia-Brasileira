import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws

GOLD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/gold/inss"))

def run(extracted_dir):
    print("[SPARK] A processar as transformações finais...")
    os.makedirs(GOLD_DIR, exist_ok=True)
    
    spark = SparkSession.builder.appName("INSSProcessor").getOrCreate()
    
    df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(f"{extracted_dir}/*.csv")
    
    # Transforma o "Ano" numa data completa de fecho de ano (Ex: 2024-12-31)
    df_transformed = df.select(
        concat_ws("-", col("ano_competencia").cast("string"), lit("12-31")).cast("date").alias("data_referencia"),
        col("uf").cast("string"),
        col("quantidade_aposentados").cast("integer").alias("total_aposentados")
    )
    
    df_transformed.write.mode("overwrite").parquet(GOLD_DIR)
    
    spark.stop()
    print("[SPARK] Ficheiros Parquet gerados na camada Gold.")
    return GOLD_DIR