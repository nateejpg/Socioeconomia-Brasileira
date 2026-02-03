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

def processar_auxilio_com_spark(caminho_csv):
    spark = SparkSession.builder \
        .appName("AuxilioBrasilETL") \
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
        found_targets = set()

        for c in df.columns:
            target = None
            
            # Lógica de Identificação
            if "UF" in c and "NOME" not in c: target = "uf"
            elif "VALOR" in c and ("PARCELA" in c or "BENEFICIO" in c): target = "valor"
            elif "NIS" in c: target = "nis"
            elif "REFER" in c or "COMPET" in c: target = "mes_ref"
            elif "MUNIC" in c and "COD" in c: target = "cod_municipio_siafi"
            elif "MUNIC" in c and "NOME" in c: target = "nome_municipio"

            if target and target not in found_targets:
                mapa[c] = target
                found_targets.add(target)

        # Seleciona apenas as colunas mapeadas
        cols_existentes = [col(k).alias(v) for k, v in mapa.items()]
        df = df.select(cols_existentes)

        # Tratamento de Valor
        if "valor" in found_targets:
            df = df.withColumn("valor", regexp_replace("valor", ",", ".").cast("float"))

        # Define agrupamento dinâmico baseado no que foi encontrado
        colunas_agrupamento = ["uf", "mes_ref"]
        if "cod_municipio_siafi" in found_targets: colunas_agrupamento.append("cod_municipio_siafi")
        if "nome_municipio" in found_targets: colunas_agrupamento.append("nome_municipio")

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
        ).withColumn("programa", lit("AUXILIO_BRASIL")).drop("mes_ref")

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
        print(f"Erro Spark: {e}")
        # Não damos raise aqui para não parar o loop do orquestrador inteiramente,
        # mas o print vai ajudar a ver se falhou de novo.
    finally:
        spark.stop()