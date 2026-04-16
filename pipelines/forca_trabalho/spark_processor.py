import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import LongType, StringType

BRONZE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/bronze/forca_trabalho"))
SILVER_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/silver/forca_trabalho"))
GOLD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/gold/forca_trabalho"))

def run(extracted_dir):
    spark = SparkSession.builder.appName("Forca_Trabalho_Processor").getOrCreate()
    
    json_file = os.path.join(extracted_dir, "forca_trabalho_records.json")
    
    if not os.path.exists(json_file):
        raise FileNotFoundError("Arquivo extraido nao encontrado.")
    
    df = spark.read.json(json_file)
    
    os.makedirs(BRONZE_DIR, exist_ok=True)
    df.write.mode("overwrite").parquet(BRONZE_DIR)
    
    os.makedirs(SILVER_DIR, exist_ok=True)
    df_silver = df.select(
        col("D3C").alias("trimestre"),
        col("D4N").alias("condicao"),
        col("V").alias("quantidade_milhares")
    ).filter(col("quantidade_milhares").isNotNull() & (col("quantidade_milhares") != "..."))
    
    df_silver.write.mode("overwrite").parquet(SILVER_DIR)
    
    os.makedirs(GOLD_DIR, exist_ok=True)
    df_gold = df_silver.select(
        col("trimestre").cast(StringType()),
        col("condicao").cast(StringType()),
        (col("quantidade_milhares").cast(LongType()) * 1000).alias("pessoas")
    ).orderBy("trimestre")
    
    df_gold.write.mode("overwrite").parquet(GOLD_DIR)
    
    spark.stop()