# Socioeconomia Brasileira

Este projeto consiste em um ecossistema de pipelines de dados automatizados para coleta, processamento e análise de indicadores socioeconômicos do Brasil. O objetivo é transformar dados brutos de fontes oficiais (como IBGE, IPEA e Banco Central) em informações estruturadas para análise.

## Arquitetura e Fluxo de Dados

O projeto foi desenhado focando em automação *serverless* e alta disponibilidade. Abaixo está o diagrama detalhado da nossa arquitetura de ponta a ponta:

![Diagrama da Arquitetura do Projeto](./images/SOCIOECON.drawio.png)


O fluxo de dados funciona nas seguintes etapas:
1. **Ingestão e Orquestração:** O GitHub Actions atua como o orquestrador (via Cron Jobs), acionando scripts em Python que extraem dados das APIs governamentais (IBGE, BCB, IPEA, Portal da Transparência).
2. **Processamento e Data Lakehouse (AWS S3):** O PySpark processa e limpa os dados, salvando-os no Amazon S3 seguindo a Arquitetura Medallion:
   - **Bronze:** Dados brutos no formato original (CSV, JSON).
   - **Silver:** Dados limpos, tipados e com tratamentos básicos de limpeza.
   - **Gold:** Dados agregados, modelados em *Star/Galaxy Schema* e prontos para consumo (Parquet).
3. **Camada de Consumo:** O Amazon Athena atua como motor de consulta *serverless*, lendo diretamente os arquivos Parquet da camada Gold no S3. O Power BI conecta-se ao Athena via driver ODBC para consumir os dados modelados e alimentar os Dashboards interativos.

## Tecnologias Utilizadas

- **Linguagem:** Python
- **Processamento:** PySpark
- **Orquestração:** Apache Airflow e GitHub Actions
- **Armazenamento e Consumo:** AWS S3 (Data Lake) e Amazon Athena (Serverless SQL)
- **Banco de Dados Local:** PostgreSQL (Metadados e testes)
- **Fontes de Dados:** API SIDRA (IBGE), IPEAData, Banco Central (SGS), Portal da Transparência.

## Estrutura do Projeto

- `dags/`: Definições das DAGs do Airflow que orquestram os pipelines (ambiente local).
- `orchestrators/`: Scripts que coordenam as etapas de cada pipeline (download, extração, processamento e carga).
- `pipelines/`: Implementação específica de cada indicador, dividida em:
  - `download.py`: Captura dos dados da fonte original.
  - `extract.py`: Conversão de formatos (ex: JSON/CSV para estruturas iniciais).
  - `spark_processor.py`: Lógica de transformação Medallion usando Spark.
  - `load.py`: Upload dos dados processados para o AWS S3.
- `data/`: Diretório local (ignorado pelo git) para armazenamento temporário durante o processamento.
- `images/`: Armazenamento de diagramas e assets de documentação.

## Como Executar

### Localmente
Para rodar um pipeline específico manualmente:
```bash
python orchestrators/nome_do_pipeline_pipeline.py