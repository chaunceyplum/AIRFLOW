from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import psycopg2
from airflow.models import Variable
import snowflake.connector
import logging
import pandas as pd
from sqlalchemy import create_engine
from airflow.utils.trigger_rule import TriggerRule
import boto3
import csv
import os
import requests
# from faker import Faker
# import random

# fake = Faker()

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
    # 'retry_delay': timedelta(minutes=2),
}

dt = datetime.today()  # Get timezone naive now
seconds = dt.timestamp()

# PostgreSQL connection ID
POSTGRES_CONN_ID = 'aws_postgres'

PG_HOST = Variable.get("PG_HOST")
PG_DATABASE = Variable.get("PG_DATABASE")
PG_USER = Variable.get("PG_USER")
PG_PASSWORD = Variable.get("PG_PASSWORD")
PG_PORT = 5432

S3_BUCKET_NAME = "baisleylake"
S3_TRANSACTION_FILE_PATH = f"airflow/{seconds}_TRANSACTION.csv"
LOCAL_TRANSACTION_FILE_PATH = "TRANSACTION.csv"
S3_CUSTOMER_FILE_PATH = f"airflow/{seconds}_CUSTOMER.csv"
LOCAL_CUSTOMER_FILE_PATH = "CUSTOMER.csv"


# AWS Credentials
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")


SNOWFLAKE_USER = Variable.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = Variable.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = Variable.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = Variable.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = Variable.get("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = Variable.get("SNOWFLAKE_SCHEMA")
SNOWFLAKE_URL = Variable.get("SNOWFLAKE_URL")

SNOWFLAKE_CONFIG = {
    "user": SNOWFLAKE_USER,
    "password": SNOWFLAKE_PASSWORD,
    "account":  SNOWFLAKE_ACCOUNT,  # "<YOUR_ACCOUNT>.snowflakecomputing.com",
    "warehouse": SNOWFLAKE_WAREHOUSE,
    "database": SNOWFLAKE_DATABASE,
    "schema": SNOWFLAKE_SCHEMA
}

def grab_customer_data(ti):
    url = "https://my.api.mockaroo.com/customer_dim.json?key=ab78c110"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()["data"]
    ti.xcom_push(key="customer_data", value=data)

def run_snowflake_query():
    engine = create_engine(
        SNOWFLAKE_URL
    )
    with engine.connect() as conn:
        result = pd.read_sql("SELECT CURRENT_TIMESTAMP;", conn)
        print(result)

# Function to insert customers
def generate_customers(ti):
    conn = psycopg2.connect(f"dbname='{PG_DATABASE}' user='{PG_USER}' host='{PG_HOST}' password='{PG_PASSWORD}'")
    cur = conn.cursor()
    customer_arr = []
    for _ in range(10):  # Generate 10 customers per run
        name_var = f'Customer_{random.randint(1, 999999999)}'
        user_var = f'user{random.randint(1000, 999999999)}@example.com'
        now = str(datetime.now())
        cur.execute("""
            INSERT INTO customers (name, email, created_at)
            VALUES (%s, %s, NOW())
        """, (name_var, user_var))
        temp_user = {"name":f'{name_var}', "email":f'{user_var}', "created_at":f'{now}'}
        customer_arr.append(temp_user)
    
    conn.commit()
    cur.close()
    conn.close()
    ti.xcom_push(key="processed_data", value=customer_arr)


# Function to insert transactions
def generate_transactions(ti):
    conn = psycopg2.connect(f"dbname='{PG_DATABASE}' user='{PG_USER}' host='{PG_HOST}' password='{PG_PASSWORD}'")
    cur = conn.cursor()
    
    cur.execute("SELECT id FROM customers ORDER BY RANDOM() LIMIT 10;")
    customers = [row[0] for row in cur.fetchall()]
    transaction_arr = []
    for customer_id in customers:
        amount_spent= round(random.uniform(1, 50000), 2)
        now = str(datetime.now())

        cur.execute("""
            INSERT INTO transactions (customer_id, amount, created_at)
            VALUES (%s, %s, NOW())
        """, (customer_id,amount_spent ))
        temp_transaction = {"customer_id":customer_id, "amount":amount_spent,"created_at":now}
        transaction_arr.append(temp_transaction)

    conn.commit()
    cur.close()
    conn.close()
    ti.xcom_push(key="processed_data", value=transaction_arr)



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



def upload_transaction_to_s3(ti):
    """Save processed data to CSV and upload to S3."""
    processed_data = ti.xcom_pull(task_ids="insert_transactions", key="processed_data")
    if not processed_data:
        print("No processed data available for CSV export")
        return

    with open(LOCAL_TRANSACTION_FILE_PATH, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["customer_id", "amount", "created_at"])
        writer.writerows(processed_data)
        

    print(f"CSV saved locally at {LOCAL_TRANSACTION_FILE_PATH}")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    s3_client.upload_file(LOCAL_TRANSACTION_FILE_PATH, S3_BUCKET_NAME, S3_TRANSACTION_FILE_PATH)
    print(f"CSV uploaded to s3://{S3_BUCKET_NAME}/{S3_TRANSACTION_FILE_PATH}")
    os.remove(LOCAL_TRANSACTION_FILE_PATH)
    print("Removed staged file")




def upload_customer_to_s3(ti):
    """Save processed data to CSV and upload to S3."""
    processed_data = ti.xcom_pull(task_ids="insert_customers", key="processed_data")
    if not processed_data:
        print("No processed data available for CSV export")
        return

    with open(LOCAL_CUSTOMER_FILE_PATH, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["name", "email", "created_at"])
        writer.writerows(processed_data)
        

    print(f"CSV saved locally at {LOCAL_CUSTOMER_FILE_PATH}")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    s3_client.upload_file(LOCAL_CUSTOMER_FILE_PATH, S3_BUCKET_NAME, S3_CUSTOMER_FILE_PATH)
    print(f"CSV uploaded to s3://{S3_BUCKET_NAME}/{S3_CUSTOMER_FILE_PATH}")
    os.remove(LOCAL_CUSTOMER_FILE_PATH)
    print("Removed staged file")

with DAG(
    'simulate_store',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Runs every 30 minutes
    catchup=False,
) as dag:

    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""

        CREATE TABLE IF NOT EXISTS customers (
            customer_id SERIAL PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            gender VARCHAR(255),
            email VARCHAR(255) UNIQUE,
            created_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id SERIAL PRIMARY KEY,
            customer_id INT REFERENCES customers(customer_id),
            product_id INT REFERENCES products(product_id),
            location_id INT REFERENCES locations(location_id),
            amount DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT NOW(),

        );

        CREATE TABLE IF NOT EXISTS products (
            product_id SERIAL PRIMARY KEY,
            product_name VARCHAR(255),
            product_amount DECIMAL(10,2),
            product_description VARCHAR(255),
            created_at TIMESTAMP DEFAULT NOW()

        );

        CREATE TABLE IF NOT EXISTS locations (
            location_id SERIAL PRIMARY KEY,
            address VARCHAR(255),
            city VARCHAR(255),
            zip_code VARCHAR(255),
            address_description VARCHAR(255),
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
    upload_transaction_csv = PythonOperator(
        task_id="upload_transaction_to_s3",
        python_callable=upload_customer_to_s3,
        # trigger_rule=TriggerRule.ALL_DONE  # Runs even if ingestion fails
    )
    upload_customer_csv = PythonOperator(
        task_id="upload_customer_to_s3",
        python_callable=upload_transaction_to_s3,
        # trigger_rule=TriggerRule.ALL_DONE  # Runs even if ingestion fails
    )
    # compute_analytics_task = PythonOperator(
    #     task_id='compute_analytics',
    #     python_callable=compute_analytics,
    # )
    # run_snowflake = PythonOperator(
    #     task_id="run_snow_query",
    #     python_callable=run_snowflake_query
    # )
   


    create_tables >> insert_customers >> insert_transactions   >> upload_customer_csv >> upload_transaction_csv
    # >> compute_analytics_task >> run_snowflake
