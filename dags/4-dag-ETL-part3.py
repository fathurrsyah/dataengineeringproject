from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
from sqlalchemy import create_engine, types
from sqlalchemy import types
from sqlalchemy import Integer, String, Float, DateTime 
import json
from fastavro import reader
from avro.datafile import DataFileReader
from avro.io import DatumReader
import copy


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 12),
}

# Step 3: Creating DAG Object
dag = DAG(dag_id='4-DAG-ETL-Part3',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False
          )

def ingest_order_items():
    with open('/opt/airflow/data/order_item.avro', 'rb') as f:
        reader = DataFileReader(f, DatumReader())
        metadata = copy.deepcopy(reader.meta)
        schema_from_file = json.loads(metadata['avro.schema'])
        
        # Batasi pembacaan hanya hingga 100,000 baris
        users = [user for _, user in zip(range(100000), reader)]
        
        reader.close()
    
    df = pd.DataFrame(users)
    df = df.drop(columns=['coupon_id'])

    pg_hook = PostgresHook(postgres_conn_id="postgres_dw")  
    engine = pg_hook.get_sqlalchemy_engine()

    df_schema = {
        "id": Integer, 
        "order_id": Integer,
        "product_id": Integer,
        "amount": Integer,
    }
    
    df.to_sql("order_items", engine, schema="public", if_exists="replace", index=False)

ingest_order_items_task = PythonOperator(
    task_id='ingest_order_items',
    python_callable=ingest_order_items,
    dag=dag)

ingest_order_items_task