# Socioeconomia Brasil - Data Pipelines

Este projeto consiste em um ecossistema de pipelines de dados automatizados para coleta, processamento e análise de indicadores socioeconômicos do Brasil. O objetivo é transformar dados brutos de fontes oficiais (como IBGE, IPEA e Banco Central) em informações estruturadas para análise.

## Arquitetura

O projeto utiliza a arquitetura Medallion para organizar os dados em diferentes estágios de maturidade:

1. Bronze: Dados brutos no formato original (Parquet).
2. Silver: Dados limpos, tipados e com tratamentos básicos de limpeza.
3. Gold: Dados agregados e prontos para consumo por ferramentas de BI ou modelos analíticos.

## Tecnologias Utilizadas

- Linguagem: Python
- Processamento: PySpark
- Orquestração: Apache Airflow e GitHub Actions
- Armazenamento: AWS S3 (Data Lake)
- Banco de Dados: PostgreSQL (Metadados e consumo)
- Fontes de Dados: API SIDRA (IBGE), IPEAData, Banco Central (SGS).

## Estrutura do Projeto

- dags/: Definições das DAGs do Airflow que orquestram os pipelines.
- orchestrators/: Scripts que coordenam as etapas de cada pipeline (download, extração, processamento e carga).
- pipelines/: Implementação específica de cada indicador, dividida em:
  - download.py: Captura dos dados da fonte original.
  - extract.py: Conversão de formatos (ex: JSON/CSV para estruturas iniciais).
  - spark_processor.py: Lógica de transformação Medallion usando Spark.
  - load.py: Upload dos dados processados para o AWS S3.
- data/: Diretório local (ignorado pelo git) para armazenamento temporário durante o processamento.

## Como Executar

### Localmente
Para rodar um pipeline específico manualmente:
python orchestrators/nome_do_pipeline_pipeline.py

### Airflow
As DAGs estão configuradas para execução periódica. Certifique-se de que o PYTHONPATH inclua a raiz do projeto.

### GitHub Actions
Os pipelines principais podem ser disparados manualmente via aba Actions ou seguindo o cronograma definido em .github/workflows/data_pipelines.yml.

## Configuração

Crie um arquivo .env na raiz do projeto com as seguintes variáveis:
- DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME
- AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_S3_BUCKET
