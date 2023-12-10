from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import script_pres

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 10),
    # Define otros argumentos según sea necesario
}

dag = DAG('dag_candidatos_presidenciales', default_args=default_args, schedule_interval=None)

def read_config_task():
    config = script_pres.read_config_file()
    if config:
        print("Archivo de configuración leído exitosamente.")
        return config  # Devuelve el config para pasarlo a la siguiente tarea
    else:
        print("Hubo un problema al leer el archivo de configuración.")
        return None

def connect_to_redshift():
    return script_pres.connect_to_redshift()  # Retorna la conexión

def load_data_to_redshift():
    config = read_config_task()  # Obtén la configuración
    conn = connect_to_redshift()  # Obtiene la conexión a Redshift
    if config and conn:
        df = script_pres.load_data()  # Carga los datos (reemplaza con tu lógica)
        script_pres.load_data_to_redshift(conn, df)  # Carga los datos a Redshift

read_config_task = PythonOperator(
    task_id='read_config_file',
    python_callable=read_config_task,
    dag=dag,
)

connect_task = PythonOperator(
    task_id='connect_to_redshift',
    python_callable=connect_to_redshift,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data_to_redshift',
    python_callable=load_data_to_redshift,
    dag=dag,
)

# Define las dependencias entre tareas
read_config_task >> connect_task >> load_data_task
