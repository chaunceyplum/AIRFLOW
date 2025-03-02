from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import psycopg2
from airflow.models import Variable

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# PostgreSQL connection ID
POSTGRES_CONN_ID = 'heroku_postgres'

PG_HOST = Variable.get("PG_HOST")
PG_DATABASE = Variable.get("PG_DATABASE")
PG_USER = Variable.get("PG_USER")
PG_PASSWORD = Variable.get("PG_PASSWORD")
PG_PORT = 5432



# Function to insert customers
def generate_customers():
    conn = psycopg2.connect(f"dbname='{PG_DATABASE}' user='{PG_USER}' host='{PG_HOST}' password='{PG_PASSWORD}'")
    cur = conn.cursor()
    
    for _ in range(10):  # Generate 10 customers per run
        cur.execute("""
            INSERT INTO customers (name, email, created_at)
            VALUES (%s, %s, NOW())
        """, (f'Customer_{random.randint(1000, 9999)}', f'user{random.randint(1000, 9999)}@example.com'))
    
    conn.commit()
    cur.close()
    conn.close()

# Function to insert transactions
def generate_transactions():
    conn = psycopg2.connect(f"dbname='{PG_DATABASE}' user='{PG_USER}' host='{PG_HOST}' password='{PG_PASSWORD}'")
    cur = conn.cursor()
    
    cur.execute("SELECT id FROM customers ORDER BY RANDOM() LIMIT 10;")
    customers = [row[0] for row in cur.fetchall()]
    
    for customer_id in customers:
        cur.execute("""
            INSERT INTO transactions (customer_id, amount, created_at)
            VALUES (%s, %s, NOW())
        """, (customer_id, round(random.uniform(5, 500), 2)))
    
    conn.commit()
    cur.close()
    conn.close()
def compute_analytics():
    conn = psycopg2.connect(f"dbname='{PG_DATABASE}' user='{PG_USER}' host='{PG_HOST}' password='{PG_PASSWORD}'")    
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS store_analytics (
            date DATE PRIMARY KEY,
            total_revenue DECIMAL(10,2),
            total_transactions INT,
            avg_transaction DECIMAL(10,2),
            unique_customers INT
        );
    """)
    
    cur.execute("""
        INSERT INTO store_analytics (date, total_revenue, total_transactions, avg_transaction, unique_customers)
        SELECT 
            DATE(created_at) AS date,
            SUM(amount) AS total_revenue,
            COUNT(*) AS total_transactions,
            AVG(amount) AS avg_transaction,
            COUNT(DISTINCT customer_id) AS unique_customers
        FROM transactions
        GROUP BY date
        ON CONFLICT (date) DO UPDATE 
        SET 
            total_revenue = EXCLUDED.total_revenue,
            total_transactions = EXCLUDED.total_transactions,
            avg_transaction = EXCLUDED.avg_transaction,
            unique_customers = EXCLUDED.unique_customers;
    """)
    
    conn.commit()
    cur.close()
    conn.close()


with DAG(
    'simulate_store',
    default_args=default_args,
    schedule_interval=timedelta(minutes=30),  # Runs every 30 minutes
    catchup=False,
) as dag:

    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS customers (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            email VARCHAR(255) UNIQUE,
            created_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            customer_id INT REFERENCES customers(id),
            amount DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
    )

    insert_customers = PythonOperator(
        task_id='insert_customers',
        python_callable=generate_customers,
    )

    insert_transactions = PythonOperator(
        task_id='insert_transactions',
        python_callable=generate_transactions,
    )
    compute_analytics_task = PythonOperator(
        task_id='compute_analytics',
        python_callable=compute_analytics,
    )



    create_tables >> insert_customers >> insert_transactions >> compute_analytics_task
