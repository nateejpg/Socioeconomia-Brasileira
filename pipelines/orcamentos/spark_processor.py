from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import DoubleType, IntegerType
import os

BRONZE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/bronze/orcamentos"))
SILVER_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/silver/orcamentos"))
GOLD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/gold/orcamentos"))

def run(extracted_dir):
    spark = SparkSession.builder.appName("Orcamento_Processor").getOrCreate()
    
    csv_files = [os.path.join(extracted_dir, f) for f in os.listdir(extracted_dir) if f.lower().endswith('.csv')]
    
    if not csv_files:
        raise FileNotFoundError(f"Nenhum arquivo CSV encontrado no diretorio: {extracted_dir}")
    
    df = spark.read.csv(csv_files, header=True, sep=";", encoding="ISO-8859-1")
    
    os.makedirs(BRONZE_DIR, exist_ok=True)
    df.write.mode("overwrite").parquet(BRONZE_DIR)
    
    os.makedirs(SILVER_DIR, exist_ok=True)
    df_silver = df.withColumnRenamed("EXERCÍCIO", "exercicio") \
                  .withColumnRenamed("NOME FUNÇÃO", "nome_funcao") \
                  .withColumnRenamed("NOME SUBFUNÇÃO", "nome_subfuncao") \
                  .withColumnRenamed("ORÇAMENTO ATUALIZADO (R$)", "orcamento_atualizado") \
                  .withColumnRenamed("ORÇAMENTO REALIZADO (R$)", "orcamento_realizado")
                  
    df_silver = df_silver.withColumn("orcamento_atualizado", regexp_replace(col("orcamento_atualizado"), "\\.", "")) \
                         .withColumn("orcamento_atualizado", regexp_replace(col("orcamento_atualizado"), ",", ".")) \
                         .withColumn("orcamento_realizado", regexp_replace(col("orcamento_realizado"), "\\.", "")) \
                         .withColumn("orcamento_realizado", regexp_replace(col("orcamento_realizado"), ",", "."))

    df_silver.write.mode("overwrite").parquet(SILVER_DIR)
    
    os.makedirs(GOLD_DIR, exist_ok=True)
    df_gold = df_silver.select(
        col("exercicio").cast(IntegerType()),
        col("nome_funcao"),
        col("nome_subfuncao"),
        col("orcamento_atualizado").cast(DoubleType()),
        col("orcamento_realizado").cast(DoubleType())
    ).filter(col("exercicio").isNotNull())
    
    df_gold.write.mode("overwrite").parquet(GOLD_DIR)
    
    spark.stop()