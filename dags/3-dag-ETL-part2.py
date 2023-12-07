import pandas as pd
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine, types
from sqlalchemy import types
from sqlalchemy import Integer, String, Float, DateTime, Boolean
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json 




default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 12),
}

# Step 3: Creating DAG Object
dag = DAG(dag_id='3-DAG-ETL-Part2',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False
          )

json_file = '/opt/airflow/data/login_attempts_0.json' 
    
def _extract():
    with open(json_file) as f:
        data = json.load(f)

    records = []
    for id in data['id']:
        records.append({
            'id': id,
            'customer_id': data['customer_id'][id],
            'login_successful': data['login_successful'][id],  
            'attempted_at': pd.to_datetime(data['attempted_at'][id], unit='ms') 
        })
    
    df = pd.DataFrame(records)  
    return df

def _transform(df):
    # Transformasi data jika diperlukan 
    return df

def _load(ds, **kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='extract_login_attempts')
    
    pg_hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = pg_hook.get_sqlalchemy_engine()

    df.to_sql("login_attempt_history", engine, schema="public", if_exists="replace", 
              index=False, 
              dtype={
                  'id': Integer,
                  'customer_id': Integer,
                  'login_successful': Boolean,
                  'attempted_at': DateTime
              })


extract_task = PythonOperator(
    task_id='extract_login_attempts',
    python_callable=_extract,
    dag=dag,
)


load_task = PythonOperator(
    task_id='load_login_attempts',
    python_callable=_load,
    provide_context=True,
    dag=dag,
)

extract_task >> load_task