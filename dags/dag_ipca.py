from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from orchestrators.ipca_pipeline import run_pipeline

default_args = {
    'owner': 'socioeconomia-brasileira',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id='dag_ipca',
    default_args=default_args,
    description='Pipeline de Inflação (IPCA - Série 433 BCB)',
    schedule_interval='0 8 1 * *',  # 8 AM on the 1st of each month
    catchup=False,
    tags=['ipca', 'inflacao'],
)

run_task = PythonOperator(
    task_id='run_ipca_pipeline',
    python_callable=run_pipeline,
    dag=dag,
)

run_task
