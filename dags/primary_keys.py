# primary_keys.py
from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 12),
}

dag = DAG(
    dag_id='DAG-MODELLING-PRIMARY-KEYS',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

add_primary_key_customers = PostgresOperator(
    task_id='add_primary_key_customers',
    sql="ALTER TABLE customers ADD PRIMARY KEY (id);",
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

# Task untuk menambahkan Primary Key pada tabel Login Attempt History
add_primary_key_login_attempt_history = PostgresOperator(
    task_id='add_primary_key_login_attempt_history',
    sql="ALTER TABLE login_attempt_history ADD PRIMARY KEY (id);",
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

# Task untuk menambahkan Primary Key pada tabel Products
add_primary_key_products = PostgresOperator(
    task_id='add_primary_key_products',
    sql="ALTER TABLE products ADD PRIMARY KEY (id);",
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

# Task untuk menambahkan Primary Key pada tabel Product Categories
add_primary_key_product_categories = PostgresOperator(
    task_id='add_primary_key_product_categories',
    sql="ALTER TABLE product_categories ADD PRIMARY KEY (id);",
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

# Task untuk menambahkan Primary Key pada tabel Suppliers
add_primary_key_suppliers = PostgresOperator(
    task_id='add_primary_key_suppliers',
    sql="ALTER TABLE suppliers ADD PRIMARY KEY (id);",
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

# Task untuk menambahkan Primary Key pada tabel Orders
add_primary_key_orders = PostgresOperator(
    task_id='add_primary_key_orders',
    sql="ALTER TABLE orders ADD PRIMARY KEY (id);",
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

# Task untuk menambahkan Primary Key pada tabel Order Items
add_primary_key_order_items = PostgresOperator(
    task_id='add_primary_key_order_items',
    sql="ALTER TABLE order_items ADD PRIMARY KEY (id);",
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

# Task untuk menambahkan Primary Key pada tabel Coupons
add_primary_key_coupons = PostgresOperator(
    task_id='add_primary_key_coupons',
    sql="ALTER TABLE coupons ADD PRIMARY KEY (id);",
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)
