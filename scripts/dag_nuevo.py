from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google_trends import fetch_google_trends_data, load_to_redshift

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 10),
    # Define otros argumentos según sea necesario
}

dag = DAG('dag_candidatos_presidenciales', default_args=default_args, schedule_interval='@daily')

fetch_data = PythonOperator(
    task_id='fetch_google_trends_data',
    python_callable=fetch_google_trends_data,
    dag=dag
)

load_to_redshift_task = PythonOperator(
    task_id='load_to_redshift',
    python_callable=load_to_redshift,
    dag=dag
)

fetch_data >> load_to_redshift_task
