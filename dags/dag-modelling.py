from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 12),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='modelling_dag',
    default_args=default_args,
    schedule_interval='@once',
) as dag:
    create_customer_detail_task = PostgresOperator(
        task_id='create_customer_detail_table',
        sql = """
            CREATE TABLE IF NOT EXISTS dim_customer_detail (
                customer_id INTEGER,
                full_name VARCHAR(255),
                gender VARCHAR(10)
            );
        """,
        postgres_conn_id='postgres_dw',
    )
    
    load_customer_detail_task = PostgresOperator(
        task_id='load_customer_detail_table',
        sql="""
            INSERT INTO dim_customer_detail (customer_id, full_name, gender)
            SELECT customer_id, CONCAT(first_name, ' ', last_name) AS full_name,
                CASE
                    WHEN gender = 'M' THEN 'Male'
                    WHEN gender = 'F' THEN 'Female'
                    ELSE NULL
                END AS gender
            FROM dim_customers;
        """,
        postgres_conn_id='postgres_dw',
    )

    create_order_transaction_task = PostgresOperator(
        task_id='create_order_transaction_table',
        sql = """
            CREATE TABLE IF NOT EXISTS dim_order_transaction (
                order_item_id INTEGER,
                transaction_date timestamp,
                product_name VARCHAR(100),
                price FLOAT,
                amount INTEGER
            );
        """,
        postgres_conn_id='postgres_dw',
    )
    
    load_order_transaction_task = PostgresOperator(
        task_id='load_order_transaction_table',
        sql="""
            INSERT INTO dim_order_transaction (order_item_id, transaction_date, product_name, price, amount)
            SELECT oi.order_items_id, o.created_at, p.name, p.price, oi.amount
            FROM dim_order_items oi
            JOIN dim_products p ON oi.product_id = p.product_id
            JOIN dim_orders o ON oi.order_id = o.orders_id;
        """,
        postgres_conn_id='postgres_dw',
    )

    # Set task dependencies
    create_customer_detail_task >> load_customer_detail_task
    create_order_transaction_task >> load_order_transaction_task

    
# import pandas as pd
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from datetime import datetime, timedelta
# from sqlalchemy import create_engine

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 1, 12),
#     'depends_on_past': False,
#     'retries': 0,
#     'retry_delay': timedelta(minutes=5),
# }

# # Define the DAG
# with DAG(
#     dag_id='modelling_dag',
#     default_args=default_args,
#     schedule_interval='@once',
# ) as dag:
# # Create the dimension tables
#     # create_transaction_task = PostgresOperator(
#     #     task_id='create_transaction_table',
#     #     sql = """
#     #         CREATE TABLE IF NOT EXISTS transactions (
#     #             transaction_id SERIAL PRIMARY KEY,
#     #             transaction_date timestamp,
#     #             product_name varchar(100),
#     #             order_id integer,
#     #             total_price float
#     #         );
#     #     """,
#     #     postgres_conn_id='postgres_dw',
#     # )
#     # load_transaction_task = PostgresOperator(
#     #     task_id='load_transaction_table',
#     #     sql="""
#     #         insert into transactions(transaction_date, product_name, order_id, total_price)
#     #         select orders.created_at,
#     #                 product.name,
#     #                 order_item.order_id,
#     #                 (product.price * order_item.amount) as total_price
#     #         FROM order_item
#     #         JOIN orders ON order_item.order_id = orders.id
#     #         JOIN product ON order_item.product_id = product.id;
#     #     """,
#     #     postgres_conn_id='postgres_dw',
#     # )

#     create_customer_detail_task = PostgresOperator(
#         task_id='create_customer_detail_table',
#         sql = """
#             CREATE TABLE IF NOT EXISTS dim_customer_detail (
#                 customer_id INTEGER,
#                 full_name VARCHAR(255),
#                 gender VARCHAR(10),
#                 FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
#             );
#         """,
#         postgres_conn_id='postgres_dw',
#     )
#     load_customer_detail_task = PostgresOperator(
#         task_id='load_customer_detail_table',
#         sql="""
#             INSERT INTO customer_detail (customer_id, full_name, gender)
#             SELECT id, CONCAT(first_name, ' ', last_name) AS full_name,
#                 CASE
#                     WHEN gender = 'M' THEN 'Male'
#                     WHEN gender = 'F' THEN 'Female'
#                     ELSE NULL
#                 END AS gender
#             FROM customer;
#         """,
#         postgres_conn_id='postgres_dw',
#     )

#     create_order_price_task = PostgresOperator(
#         task_id='create_order_price_table',
#         sql = """
#             CREATE TABLE IF NOT EXISTS order_price (
#                 order_item_id INTEGER,
#                 product_name VARCHAR(100),
#                 price FLOAT,
#                 amount INTEGER,
#                 FOREIGN KEY (order_item_id) REFERENCES order_item(id)
#             );
#         """,
#         postgres_conn_id='postgres_dw',
#     )
#     load_order_price_task = PostgresOperator(
#         task_id='load_order_price_table',
#         sql="""
#             INSERT INTO order_price (order_item_id, product_name, price, amount)
#             SELECT order_item.id, product.name, product.price, order_item.amount
#             FROM order_item
#             JOIN product ON order_item.product_id = product.id;
#         """,
#         postgres_conn_id='postgres_dw',
#     )

# # # create the fact table
# #     create_sales_information_task = PostgresOperator(
# #         task_id='create_sales_information_table',
# #         sql="""
# #             CREATE TABLE IF NOT EXISTS sales_information (
# #                 id SERIAL PRIMARY KEY,
# #                 customer_id INTEGER,
# #                 order_id INTEGER,
# #                 product_id INTEGER,
# #                 category_id INTEGER,
# #                 supplier_id INTEGER,
# #                 transaction_id INTEGER,
# #                 FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
# #             );
# #         """,
# #         postgres_conn_id='postgres_dw', 
# #     )

# #     # Load data into the sales_information table
# #     load_sales_information_task = PostgresOperator(
# #         task_id='load_sales_information_table',
# #         sql="""
# #             INSERT INTO sales_information (
# #                 customer_id, order_id, product_id, 
# #                 category_id, supplier_id, transaction_id
# #             )
# #             SELECT orders.customer_id, 
# #                    order_item.order_id,
# #                    order_item.product_id,
# #                    product.category_id,
# #                    product.supplier_id,
# #                    transactions.transaction_id
# #             FROM order_item
# #             JOIN orders ON order_item.order_id = orders.id
# #             JOIN product ON order_item.product_id = product.id
# #             JOIN transactions ON order_item.order_id = transactions.order_id;
# #         """, 
# #         postgres_conn_id='postgres_dw',
# #     )

#     # Set task dependencies
# # create_transaction_task >> load_transaction_task >> create_sales_information_task >> load_sales_information_task
# create_customer_detail_task >> load_customer_detail_task
# create_order_price_task >> load_order_price_task