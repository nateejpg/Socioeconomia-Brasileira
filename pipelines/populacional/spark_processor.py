import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, LongType

BRONZE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/bronze/populacao"))
SILVER_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/silver/populacao"))
GOLD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/gold/populacao"))

def run(extracted_dir):
    spark = SparkSession.builder.appName("Populacao_Processor").getOrCreate()
    
    json_file = os.path.join(extracted_dir, "populacao_records.json")
    
    if not os.path.exists(json_file):
        raise FileNotFoundError("Arquivo extraido nao encontrado.")
    
    df = spark.read.json(json_file)
    
    os.makedirs(BRONZE_DIR, exist_ok=True)
    df.write.mode("overwrite").parquet(BRONZE_DIR)
    
    os.makedirs(SILVER_DIR, exist_ok=True)
    df_silver = df.select(
        col("date").alias("ano"),
        col("value").alias("populacao")
    ).filter(col("populacao").isNotNull())
    
    df_silver.write.mode("overwrite").parquet(SILVER_DIR)
    
    os.makedirs(GOLD_DIR, exist_ok=True)
    df_gold = df_silver.select(
        col("ano").cast(IntegerType()),
        col("populacao").cast(LongType())
    ).orderBy("ano")
    
    df_gold.write.mode("overwrite").parquet(GOLD_DIR)
    
    spark.stop()