import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, to_date, concat, lit, substring, regexp_replace
from dotenv import load_dotenv

load_dotenv(override=True)

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')

def processar_com_spark(caminho_csv):
    spark = SparkSession.builder \
        .appName("BolsaFamiliaETL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = spark.read.option("header", "true") \
            .option("delimiter", ";") \
            .option("encoding", "ISO-8859-1") \
            .csv(caminho_csv)

        colunas_normalizadas = [c.strip().upper() for c in df.columns]
        df = df.toDF(*colunas_normalizadas)

        mapa = {}
        for c in df.columns:
            if "UF" in c and "NOME" not in c: mapa[c] = "uf"
            elif "VALOR PARCELA" in c: mapa[c] = "valor"
            elif "NIS" in c: mapa[c] = "nis"
            elif "REFER" in c: mapa[c] = "mes_ref"
            elif "MUNIC" in c and "COD" in c: mapa[c] = "cod_municipio_siafi"
            elif "MUNIC" in c and "NOME" in c: mapa[c] = "nome_municipio"
        
        cols_existentes = [col(k).alias(v) for k, v in mapa.items()]
        df = df.select(cols_existentes)

        df = df.withColumn("valor", regexp_replace("valor", ",", ".").cast("float"))

        colunas_agrupamento = ["uf", "mes_ref"]
        if "cod_municipio_siafi" in mapa.values():
            colunas_agrupamento.append("cod_municipio_siafi")
        if "nome_municipio" in mapa.values():
            colunas_agrupamento.append("nome_municipio")

        df_agregado = df.groupBy(*colunas_agrupamento) \
            .agg(
                sum("valor").alias("valor_pago"),
                count("nis").alias("quantidade_beneficiarios")
            )

        df_final = df_agregado.withColumn(
            "referencia_data",
            to_date(
                concat(
                    substring(col("mes_ref"), 1, 4),
                    lit("-"),
                    substring(col("mes_ref"), 5, 2),
                    lit("-01")
                )
            )
        ).drop("mes_ref")

        jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
        properties = {
            "user": DB_USER,
            "password": DB_PASS,
            "driver": "org.postgresql.Driver"
        }

        df_final.write.jdbc(
            url=jdbc_url,
            table="fato_bolsa_familia",
            mode="append",
            properties=properties
        )

    except Exception as e:
        print(f"Erro no Spark: {e}")
        raise e
    finally:
        spark.stop()