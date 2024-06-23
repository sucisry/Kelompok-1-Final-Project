from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from sqlalchemy.engine import URL, create_engine

def postgres_connection():
    connection_string = URL.create(
        'postgresql',
        username='data_warehouse_owner',
        password='SwEm1h0bTgWz',
        host='ep-lively-tooth-a1rqoefa.ap-southeast-1.aws.neon.tech',
        database='data_warehouse',
        port=5432,
        query={'sslmode': 'require'}
    )
    engine = create_engine(connection_string)
    return engine

# Fungsi untuk membuat data mart Sales Summary
def sales_summary():
    engine = postgres_connection()
    query = """
    SELECT 
        o.id AS order_id,
        o.created_at AS order_date,
        c.id AS customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        p.id AS product_id,
        p.name AS product_name,
        pc.name AS product_category,
        oi.amount AS quantity,
        p.price * oi.amount AS total_price,
        COALESCE(cp.discount_percent, 0) AS discount_percent,
        (p.price * oi.amount) * (1 - COALESCE(cp.discount_percent, 0)/100) AS discounted_price
    FROM 
        orders o
        JOIN customer c ON o.customer_id = c.id
        JOIN order_items oi ON o.id = oi.order_id
        JOIN products p ON oi.product_id = p.id
        JOIN product_categories pc ON p.category_id = pc.id
        LEFT JOIN coupons cp ON oi.coupon_id = cp.id
    """
    df = pd.read_sql(query, engine)
    df.to_sql('data_mart_sales_summary', engine, if_exists='replace', index=False)

# Fungsi untuk membuat data mart Customer Summary
def customer_summary():
    engine = postgres_connection()
    query = """
    SELECT 
        c.id AS customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        c.gender,
        c.zip_code,
        COUNT(DISTINCT o.id) AS total_orders,
        SUM(p.price * oi.amount) AS total_spent,
        AVG(p.price * oi.amount) AS average_order_value,
        MAX(o.created_at) AS last_order_date,
        COUNT(DISTINCT CASE WHEN lah.login_successful THEN lah.id END) AS successful_logins,
        COUNT(DISTINCT CASE WHEN NOT lah.login_successful THEN lah.id END) AS failed_logins
    FROM 
        customer c
        LEFT JOIN orders o ON c.id = o.customer_id
        LEFT JOIN order_items oi ON o.id = oi.order_id
        LEFT JOIN products p ON oi.product_id = p.id
        LEFT JOIN login_attemps lah ON c.id = lah.customer_id
    GROUP BY 
        c.id, c.first_name, c.last_name, c.gender, c.zip_code
    """
    df = pd.read_sql(query, engine)
    df.to_sql('data_mart_customer_summary', engine, if_exists='replace', index=False)

# Fungsi untuk membuat data mart Product Performance
def product_performance():
    engine = postgres_connection()
    query = """
    SELECT 
        p.id AS product_id,
        p.name AS product_name,
        pc.name AS category_name,
        s.name AS supplier_name,
        s.country AS supplier_country,
        COUNT(DISTINCT oi.order_id) AS total_orders,
        SUM(oi.amount) AS total_quantity_sold,
        SUM(p.price * oi.amount) AS total_revenue,
        AVG(p.price) AS average_price
    FROM 
        products p
        JOIN product_categories pc ON p.category_id = pc.id
        JOIN suppliers s ON p.supplier_id = s.id
        LEFT JOIN order_items oi ON p.id = oi.product_id
    GROUP BY 
        p.id, p.name, pc.name, s.name, s.country
    """
    df = pd.read_sql(query, engine)
    df.to_sql('data_mart_product_performance', engine, if_exists='replace', index=False)

# Fungsi untuk membuat data mart Login Summary
def login_summary():
    engine = postgres_connection()
    query = """
    SELECT 
        c.id AS customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        COUNT(lah.id) AS total_login_attemps,
        SUM(CASE WHEN lah.login_successful THEN 1 ELSE 0 END) AS successful_logins,
        SUM(CASE WHEN NOT lah.login_successful THEN 1 ELSE 0 END) AS failed_logins,
        MAX(lah.attempted_at) AS last_login_attempt
    FROM 
        customer c
        LEFT JOIN login_attemps lah ON c.id = lah.customer_id
    GROUP BY 
        c.id, c.first_name, c.last_name
    """
    df = pd.read_sql(query, engine)
    df.to_sql('data_mart_login_summary', engine, if_exists='replace', index=False)

# Fungsi untuk membuat data mart Coupon Effectiveness
def coupon_effectiveness():
    engine = postgres_connection()
    query = """
    SELECT 
        cp.id AS coupon_id,
        cp.discount_percent,
        COUNT(DISTINCT oi.order_id) AS orders_used,
        COUNT(DISTINCT oi.product_id) AS products_discounted,
        SUM(p.price * oi.amount) AS total_pre_discount_value,
        SUM(p.price * oi.amount * (1 - cp.discount_percent/100)) AS total_post_discount_value,
        SUM(p.price * oi.amount * (cp.discount_percent/100)) AS total_discount_amount
    FROM 
        coupons cp
        JOIN order_items oi ON cp.id = oi.coupon_id
        JOIN products p ON oi.product_id = p.id
    GROUP BY 
        cp.id, cp.discount_percent
    """
    df = pd.read_sql(query, engine)
    df.to_sql('data_mart_coupon_effectiveness', engine, if_exists='replace', index=False)

# Konfigurasi DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_marts_dag',
    default_args=default_args,
    description='DAG untuk membuat data mart',
    schedule_interval='@once',
)

# Definisikan task untuk setiap data mart
create_sales_summary_task = PythonOperator(
    task_id='sales_summary',
    python_callable=sales_summary,
    dag=dag,
)

create_customer_summary_task = PythonOperator(
    task_id='customer_summary',
    python_callable=customer_summary,
    dag=dag,
)

create_product_performance_task = PythonOperator(
    task_id='product_performance',
    python_callable=product_performance,
    dag=dag,
)

create_login_summary_task = PythonOperator(
    task_id='login_summary',
    python_callable=login_summary,
    dag=dag,
)

create_coupon_effectiveness_task = PythonOperator(
    task_id='coupon_effectiveness',
    python_callable=coupon_effectiveness,
    dag=dag,
)

# Atur urutan eksekusi task
create_sales_summary_task >> create_customer_summary_task >> create_product_performance_task >> create_login_summary_task >> create_coupon_effectiveness_task
