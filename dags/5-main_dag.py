# main_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from primary_keys import dag as primary_keys_dag
from foreign_keys import dag as foreign_keys_dag

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 12),
}

dag = DAG(
    dag_id='5-DAG-MODELLING-MAIN',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

# Trigger primary_keys_dag first
trigger_primary_keys = TriggerDagRunOperator(
    task_id='trigger_primary_keys',
    trigger_dag_id='DAG-MODELLING-PRIMARY-KEYS',
    dag=dag,
)

# Trigger foreign_keys_dag after primary_keys_dag completes
trigger_foreign_keys = TriggerDagRunOperator(
    task_id='trigger_foreign_keys',
    trigger_dag_id='DAG-MODELLING-FOREIGN-KEYS',
    dag=dag,
)

def print_hello():
    print("Hello from main DAG!")

# Just a sample task in the main DAG
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

trigger_primary_keys >> trigger_foreign_keys >> hello_task
