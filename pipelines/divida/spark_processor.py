import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../data"))

def run(csv_path):
    spark = SparkSession.builder.appName("Divida_Medallion").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("[Spark] Lendo arquivo extraído...")
    df_raw = spark.read.option("header", "true").csv(csv_path)

    bronze_path = os.path.join(DATA_DIR, "bronze/divida")
    df_raw.write.mode("overwrite").parquet(bronze_path)

    # SILVER: Multiplicando por 1.000.000 para virar Reais absolutos
    print("[Spark] Processando Camada Silver...")
    df_silver = df_raw.withColumn("data_ref", F.to_date(F.col("data"), "dd/MM/yyyy")) \
                      .withColumn("ano", F.year(F.col("data_ref"))) \
                      .withColumn("mes", F.month(F.col("data_ref"))) \
                      .withColumn("valor_divida", F.col("valor").cast("double") * 1000000) \
                      .drop("data", "valor")
    
    silver_path = os.path.join(DATA_DIR, "silver/divida")
    df_silver.write.mode("overwrite").parquet(silver_path)

    # GOLD: Filtrando apenas Dezembro (a "fotografia" anual da dívida)
    print("[Spark] Processando Camada Gold...")
    df_gold = df_silver.filter(F.col("mes") == 12) \
                       .select("ano", "valor_divida")
    
    gold_path = os.path.join(DATA_DIR, "gold/divida")
    df_gold.write.mode("overwrite").parquet(gold_path)
    
    print(f"-> Sucesso no Spark! Total de anos processados na Gold: {df_gold.count()}")

if __name__ == "__main__":
    pass