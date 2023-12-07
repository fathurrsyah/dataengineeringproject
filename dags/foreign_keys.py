# foreign_keys.py
from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 12),
}

dag = DAG(
    dag_id='DAG-MODELLING-FOREIGN-KEYS',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

# Task untuk menambahkan Foreign Key Constraint pada tabel Orders
add_foreign_key_orders = PostgresOperator(
    task_id='add_foreign_key_orders',
    sql="ALTER TABLE orders ADD CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES customers(id);",
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

# Task untuk menambahkan Foreign Key Constraint pada tabel Order Items
add_foreign_key_order_items = PostgresOperator(
    task_id='add_foreign_key_order_items',
    sql="ALTER TABLE order_items ADD CONSTRAINT fk_order_id FOREIGN KEY (order_id) REFERENCES orders(id);"
        "ALTER TABLE order_items ADD CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES products(id);",
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)

# Task untuk menambahkan Foreign Key Constraint pada tabel Products
add_foreign_key_products = PostgresOperator(
    task_id='add_foreign_key_products',
    sql="ALTER TABLE products ADD CONSTRAINT fk_category_id FOREIGN KEY (category_id) REFERENCES product_categories(id);"
        "ALTER TABLE products ADD CONSTRAINT fk_supplier_id FOREIGN KEY (supplier_id) REFERENCES suppliers(id);",
    postgres_conn_id='postgres_dw',
    autocommit=True,
    dag=dag,
)
