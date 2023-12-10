from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
import json
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG('insert_google_trends_to_redshift', default_args=default_args, schedule_interval='@daily')
dag_path = os.getcwd()
print(dag_path)

def fetch_google_trends_data():
    csv_directory = dag_path + "/ext/" + "candidatos.csv"
    df = pd.read_csv(csv_directory)

    return df

def save_to_redshift():
    iot = fetch_google_trends_data()
    with open(dag_path + "/ext/" + 'config.json', 'r') as f:
        config = json.load(f)
        
    conn = psycopg2.connect(
        dbname=config['dbname'],
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['password']
    )

    cur = conn.cursor()

    for index, row in iot.iterrows():
        fecha = index.strftime('%Y-%m-%d')
        query = """
            INSERT INTO candidatos_presidenciales_trends 
            (fecha, javier_milei, sergio_massa, patricia_bullrich, juan_schiaretti, myriam_bregman, is_partial) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        values = (fecha, row['Javier Milei'], row['Sergio Massa'], row['Patricia Bullrich'],
                  row['Juan Schiaretti'], row['Myriam Bregman'], row['isPartial'])

        cur.execute(query, values)
    
    conn.commit()
    cur.close()
    conn.close()


# Tarea para obtener datos de Google Trends
fetch_google_trends_task = PythonOperator(
    task_id='fetch_google_trends_data',
    python_callable=fetch_google_trends_data,
    dag=dag,
)

# Tarea para guardar en Redshift
save_to_redshift_task = PythonOperator(
    task_id='save_to_redshift',
    python_callable=save_to_redshift,
    dag=dag,
)

fetch_google_trends_task >> save_to_redshift_task
