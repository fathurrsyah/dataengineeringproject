from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

# Default_args digunakan untuk mendefinisikan konfigurasi DAG yang umum
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 12),
}

# Inisialisasi DAG
dag = DAG(
    '1-dag-create-table',
    default_args=default_args,
    description='DAG to create PostgreSQL tables with references',
    schedule_interval='@once',  # Atur interval sesuai kebutuhan
)

# Define SQL statements for table creation
create_customers_sql = """
DROP TABLE IF EXISTS customers;

CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    address VARCHAR(255),
    gender VARCHAR(255),
    zip_code VARCHAR(255)
);
"""

create_login_attempt_history_sql = """
DROP TABLE IF EXISTS login_attempts;

CREATE TABLE IF NOT EXISTS login_attempt_history (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    login_successful BOOLEAN,
    attempted_at TIMESTAMP
);
"""

create_products_sql = """
DROP TABLE IF EXISTS products;

CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    price FLOAT,
    category_id INTEGER,
    supplier_id INTEGER
);
"""

create_product_categories_sql = """
DROP TABLE IF EXISTS product_categories;

CREATE TABLE IF NOT EXISTS product_categories (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255)
);
"""

create_suppliers_sql = """
DROP TABLE IF EXISTS suppliers;

CREATE TABLE IF NOT EXISTS suppliers (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    country VARCHAR(255)
);
"""

create_orders_sql = """
DROP TABLE IF EXISTS orders;

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY,
    customer_id VARCHAR(255),
    status TEXT,
    created_at TIMESTAMP
);
"""

create_order_items_sql = """
DROP TABLE IF EXISTS order_items;

CREATE TABLE IF NOT EXISTS order_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    amount INTEGER
);
"""

create_coupons_sql = """
DROP TABLE IF EXISTS coupons;

CREATE TABLE IF NOT EXISTS coupons (
    id INTEGER PRIMARY KEY,
    discount_percent FLOAT
);
"""

# Define tasks for each table creation
create_customers_task = PostgresOperator(
    task_id='create_customers_table',
    sql=create_customers_sql,
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

create_login_attempt_history_task = PostgresOperator(
    task_id='create_login_attempt_history_table',
    sql=create_login_attempt_history_sql,
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

create_products_task = PostgresOperator(
    task_id='create_products_table',
    sql=create_products_sql,
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

create_product_categories_task = PostgresOperator(
    task_id='create_product_categories_table',
    sql=create_product_categories_sql,
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

create_suppliers_task = PostgresOperator(
    task_id='create_suppliers_table',
    sql=create_suppliers_sql,
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

create_orders_task = PostgresOperator(
    task_id='create_orders_table',
    sql=create_orders_sql,
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

create_order_items_task = PostgresOperator(
    task_id='create_order_items_table',
    sql=create_order_items_sql,
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

create_coupons_task = PostgresOperator(
    task_id='create_coupons',
    sql=create_coupons_sql, 
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

# Set task dependencies
# Set task dependencies
create_product_categories_task >> create_products_task
create_suppliers_task >> create_products_task
create_customers_task >> create_login_attempt_history_task >> create_orders_task >> create_order_items_task
create_orders_task >> create_order_items_task
create_coupons_task >> create_order_items_task