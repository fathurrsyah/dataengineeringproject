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
    dag_id='7-DAG-MODELLING',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

# Task untuk membuat tabel revenue_per_category
create_revenue_per_category_table = PostgresOperator(
    task_id='create_revenue_per_category_table',
    sql="""
    CREATE TABLE IF NOT EXISTS revenue_per_category AS
    SELECT p.category_id, extract(month from o.created_at) AS month,  
           SUM(oi.amount*p.price) AS revenue
    FROM orders o JOIN order_items oi ON o.id = oi.order_id
                 JOIN products p ON oi.product_id = p.id
    GROUP BY 1,2
    ORDER BY 1,2;
    """,
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

# Definisi ketergantungan antar tugas
create_revenue_per_category_table