from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from orchestrators.divida_pipeline import run_pipeline

default_args = {
    'owner': 'socioeconomia-brasileira',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id='dag_divida',
    default_args=default_args,
    description='Pipeline de Dívida',
    schedule_interval='0 4 1 * *',  # 4 AM on the 1st of each month
    catchup=False,
    tags=['divida'],
)

run_task = PythonOperator(
    task_id='run_divida_pipeline',
    python_callable=run_pipeline,
    dag=dag,
)

run_task
