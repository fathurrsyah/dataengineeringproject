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
    dag_id='6-DAG-MODELLING',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

# Task untuk membuat tabel sales_per_category
create_sales_per_category_table = PostgresOperator(
    task_id='create_sales_per_category_table',
    sql="""
    CREATE TABLE IF NOT EXISTS sales_per_category AS
    SELECT 
      p.category_id,
      c.name AS category,
      SUM(oi.amount * p.price) AS total_sales
    FROM order_items oi
    INNER JOIN products p ON oi.product_id = p.id  
    INNER JOIN product_categories c ON c.id = p.category_id
    GROUP BY p.category_id, c.name;
    """,
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

# Definisi ketergantungan antar tugas
create_sales_per_category_table