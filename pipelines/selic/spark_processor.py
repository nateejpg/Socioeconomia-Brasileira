import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../data"))

def run(csv_path):
    spark = SparkSession.builder.appName("Selic_Medallion").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("[Spark] Lendo ficheiro extraído...")
    df_raw = spark.read.option("header", "true").csv(csv_path)

    bronze_path = os.path.join(DATA_DIR, "bronze/selic")
    df_raw.write.mode("overwrite").parquet(bronze_path)

    print("[Spark] Processando Camada Silver...")
    # A API do BCB já envia os decimais com ponto (ex: 10.50), logo o cast direto funciona perfeitamente.
    df_silver = df_raw.withColumn("data_ref", F.to_date(F.col("data"), "dd/MM/yyyy")) \
                      .withColumn("ano", F.year(F.col("data_ref"))) \
                      .withColumn("mes", F.month(F.col("data_ref"))) \
                      .withColumn("valor_selic", F.col("valor").cast("double")) \
                      .drop("data", "valor")
    
    silver_path = os.path.join(DATA_DIR, "silver/selic")
    df_silver.write.mode("overwrite").parquet(silver_path)

    print("[Spark] Processando Camada Gold...")
    df_gold = df_silver.filter(F.col("valor_selic").isNotNull()) \
                       .select("data_ref", "ano", "mes", "valor_selic")
    
    gold_path = os.path.join(DATA_DIR, "gold/selic")
    df_gold.write.mode("overwrite").parquet(gold_path)
    
    print(f"-> Sucesso no Spark! Total de registos salvos na Gold: {df_gold.count()}")

if __name__ == "__main__":
    pass