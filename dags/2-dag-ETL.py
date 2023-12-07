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


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 12),
}

#Creating DAG Object
dag = DAG(dag_id='2-DAG-ETL-Part1',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False
          )

def ingest_customers():
    pg_hook = PostgresHook(postgres_conn_id="postgres_dw") 
    engine = pg_hook.get_sqlalchemy_engine()

    #Extract
    df = pd.concat([pd.read_csv(f"/opt/airflow/data/customer_{i}.csv") for i in range(10)])

    #Transform
    df.drop("Unnamed: 0", axis=1, inplace=True)

    df_schema = {
        "id": Integer, 
        "first_name": String,
        "last_name": String,
        "gender": String,
        "address": String, 
        "zip_code": Integer  
    }
    #Load
    df.to_sql("customers", engine, schema="public", if_exists="replace", index=False, dtype=df_schema)





def ingest_products():
    df = pd.read_excel("/opt/airflow/data/product.xls")
    df.drop("Unnamed: 0", axis=1, inplace=True)

    pg_hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = pg_hook.get_sqlalchemy_engine()

    df_schema = { 
        "id": Integer,
        "name": String,
        "price": Float,
        "category_id": Integer, 
        "supplier_id": Integer
    }
    
    df.to_sql("products", engine, schema="public", if_exists="replace", index=False, dtype=df_schema)

def ingest_product_categories():
    df = pd.read_excel("/opt/airflow/data/product_category.xls")
    df.drop("Unnamed: 0", axis=1, inplace=True)

    pg_hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = pg_hook.get_sqlalchemy_engine()

    df_schema = {
        "id": Integer,
        "name": String
    }
    
    df.to_sql("product_categories", engine, schema="public", if_exists="replace", index=False, dtype=df_schema)

def ingest_suppliers():
    #extract
    df = pd.read_excel("/opt/airflow/data/supplier.xls")

    #transform
    df.drop("Unnamed: 0", axis=1, inplace=True)

    pg_hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = pg_hook.get_sqlalchemy_engine()

    df_schema = {
        "id": Integer, 
        "name": String,
        "country": String 
    }
    
    #load
    df.to_sql("suppliers", engine, schema="public", if_exists="replace", index=False, dtype=df_schema)

def ingest_orders():
    df = pd.read_parquet("/opt/airflow/data/order.parquet")
    df['status'] = df['status'].replace({
        'RECEIVED': 'RECEIVED',
        'FINISHED': 'FINISHED',
        'SENT': 'SENT',
        'PROCESSED': 'PROCESSED',
        'RETURNED': 'RETURN',
        'ABORTED': 'ABORTED'
        })
    
    pg_hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = pg_hook.get_sqlalchemy_engine()

    df_schema = {
        "id": Integer, 
        "customer_id": String,
        "status": String,  
        "created_at": DateTime   
    }
    
    df.to_sql("orders", engine, schema="public", if_exists="replace", index=False, dtype=df_schema)



def ingest_coupons():
    with open('/opt/airflow/data/coupons.json') as f:
        data = json.load(f)
    
    df = pd.DataFrame(columns=['id', 'discount_percent'])
    new_row = {'id': '11', 'discount_percent': 0}
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    
    for i in range(10):
    # Append data
        id = int(data['id'][str(i)])
        discount_percent = data['discount_percent'][str(i)] / 100

        new_row = pd.DataFrame([[id, discount_percent]], columns=['id', 'discount_percent'])
        df = pd.concat([df, new_row], ignore_index=True)


    pg_hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = pg_hook.get_sqlalchemy_engine()

    df_schema = {
        "id": Integer,  
        "discount_percent": Float
    }
    
    df.to_sql("coupons", engine, schema="public", if_exists="replace", index=False, dtype=df_schema)




ingest_customers = PythonOperator(
    task_id='ingest_customers',
    python_callable=ingest_customers,
    dag=dag)



ingest_products = PythonOperator(
    task_id='ingest_products',
    python_callable=ingest_products, 
    dag=dag)

ingest_product_categories = PythonOperator(
    task_id='ingest_product_categories',
    python_callable=ingest_product_categories,
    dag=dag) 

ingest_suppliers = PythonOperator(
    task_id='ingest_suppliers',
    python_callable=ingest_suppliers,
    dag=dag)

ingest_orders = PythonOperator(  
    task_id='ingest_orders',
    python_callable=ingest_orders,
    dag=dag)

ingest_coupons = PythonOperator(
    task_id='ingest_coupons',
    python_callable=ingest_coupons, 
    dag=dag)

# ingest_customers >> ingest_login_attempts >> ingest_products >> ingest_product_categories >> ingest_suppliers >> ingest_orders >> ingest_order_items >> ingest_coupons
ingest_customers >>  ingest_products >> ingest_product_categories >> ingest_suppliers >> ingest_orders >> ingest_coupons