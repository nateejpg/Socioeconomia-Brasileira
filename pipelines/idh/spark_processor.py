from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, create_map
from pyspark.sql.types import DoubleType, IntegerType
from itertools import chain
import os

BRONZE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/bronze/idh"))
SILVER_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/silver/idh"))
GOLD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/gold/idh"))

MAPA_IBGE = {
    'Rondônia': 11, 'Acre': 12, 'Amazonas': 13, 'Roraima': 14, 'Pará': 15, 'Amapá': 16, 'Tocantins': 17,
    'Maranhão': 21, 'Piauí': 22, 'Ceará': 23, 'Rio Grande do Norte': 24, 'Paraíba': 25, 'Pernambuco': 26, 'Alagoas': 27, 'Sergipe': 28, 'Bahia': 29,
    'Minas Gerais': 31, 'Espírito Santo': 32, 'Rio de Janeiro': 33, 'São Paulo': 35,
    'Paraná': 41, 'Santa Catarina': 42, 'Rio Grande do Sul': 43,
    'Mato Grosso do Sul': 50, 'Mato Grosso': 51, 'Goiás': 52, 'Distrito Federal': 53
}

def run(csv_path):
    spark = SparkSession.builder.appName("IDH_Processor").getOrCreate()
    
    # O CSV agora já chega no formato correto: estado_limpo, ano, idhm, idhm_e, idhm_l, idhm_r
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    
    os.makedirs(BRONZE_DIR, exist_ok=True)
    df.write.mode("overwrite").parquet(BRONZE_DIR)
    
    os.makedirs(SILVER_DIR, exist_ok=True)
    
    mapping_expr = create_map([lit(x) for x in chain(*MAPA_IBGE.items())])
    
    # Camada Silver cruza o mapa e garante que as linhas não cruzadas sejam dropadas
    df_silver = df.withColumn("cod_ibge", mapping_expr[col("estado_limpo")]) \
                  .filter(col("cod_ibge").isNotNull()) \
                  .withColumn("nivel_territorial", lit("Estados"))

    df_silver.write.mode("overwrite").parquet(SILVER_DIR)
    
    os.makedirs(GOLD_DIR, exist_ok=True)
    
    # Camada Gold apenas organiza e tipa
    df_gold = df_silver.select(
        col("cod_ibge").cast(IntegerType()),
        col("nivel_territorial"),
        col("ano").cast(IntegerType()),
        col("idhm").cast(DoubleType()).alias("valor_idhm"),
        col("idhm_e").cast(DoubleType()).alias("idh_educacao"),
        col("idhm_l").cast(DoubleType()).alias("idh_longevidade"),
        col("idhm_r").cast(DoubleType()).alias("idh_renda")
    )
    
    df_gold.write.mode("overwrite").parquet(GOLD_DIR)
    
    total_gold = df_gold.count()
    print(f"-> Sucesso no Spark! Total de registros salvos na Gold: {total_gold}")
    
    spark.stop()