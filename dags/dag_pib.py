from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from orchestrators.pib_pipeline import run_pipeline

default_args = {
    'owner': 'socioeconomia-brasileira',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    dag_id='dag_pib',
    default_args=default_args,
    description='Pipeline de PIB (Produto Interno Bruto)',
    schedule_interval='0 10 1 * *',  # 10 AM on the 1st of each month
    catchup=False,
    tags=['pib'],
)

run_task = PythonOperator(
    task_id='run_pib_pipeline',
    python_callable=run_pipeline,
    dag=dag,
)

run_task
