from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

# Tanggal awal dan parameter default
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 12),
}

# Creating DAG Object
dag = DAG(
    dag_id='8-DAG-MODELLING',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

# Task untuk membuat tabel return_rate_per_category
create_return_rate_per_category_table = PostgresOperator(
    task_id='create_return_rate_per_category_table',
    sql="""
    CREATE TABLE IF NOT EXISTS return_rate_per_category AS
    SELECT p.category_id, COUNT(*) AS total_orders,
           SUM(CASE WHEN o.status = 'RETURN' THEN 1 ELSE 0 END) AS total_returned,
           AVG(CASE WHEN o.status = 'RETURN' THEN 1 ELSE 0 END) AS return_rate
    FROM order_items oi JOIN orders o ON oi.order_id = o.id     
                 JOIN products p ON oi.product_id = p.id
    GROUP BY 1;
    """,
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

# Definisi ketergantungan antar tugas
create_return_rate_per_category_table
