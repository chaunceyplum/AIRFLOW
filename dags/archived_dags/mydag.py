from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import requests
import psycopg2
import boto3
import csv
import os
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
from sqlalchemy import create_engine

dt = datetime.today()  # Get timezone naive now
seconds = dt.timestamp()

# PostgreSQL connection details
PG_HOST = Variable.get("PG_HOST")
# PG_DATABASE = Variable.get("PG_DATABASE")
PG_DATABASE = "crypto"
PG_USER = Variable.get("PG_USER")
PG_PASSWORD = Variable.get("PG_PASSWORD")
PG_PORT = 5432

# S3 configuration
S3_BUCKET_NAME = "baisleylake"
S3_FILE_PATH = f"airflow/{seconds}_coincap_data.csv"
LOCAL_FILE_PATH = "coincap_data.csv"

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


def run_snowflake_query(ti):
    processed_data = ti.xcom_pull(task_ids="process_crypto_data", key="processed_data")
    engine = create_engine(
        SNOWFLAKE_URL
    )
    with engine.connect() as conn:
        create_table = pd.read_sql("""
            CREATE TABLE IF NOT EXISTS assets (
                id TEXT,
                rank TEXT,
                symbol TEXT,
                name TEXT,
                price_usd FLOAT,
                timestamp TIMESTAMP, 
                PRIMARY KEY(symbol, timestamp)
            );
        """, conn)
        print(create_table)
        insert_assets = pd.read_sq()


def fetch_coincap_data(ti):
    """Fetch data from CoinCap API and push to XCom."""
    url = "https://api.coincap.io/v2/assets"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()["data"]
    ti.xcom_push(key="coincap_data", value=data)

def process_data(ti):
    """Extract relevant fields from CoinCap API response."""
    coincap_data = ti.xcom_pull(task_ids="fetch_crypto_data", key="coincap_data")
    if not coincap_data:
        raise ValueError("No data received from CoinCap API")

    processed_data = [
        (item["id"], item["rank"], item["symbol"], item["name"], float(item["priceUsd"]), datetime.now())
        for item in coincap_data
    ]
    ti.xcom_push(key="processed_data", value=processed_data)

def ingest_data_to_postgres(ti):
    """Ingest processed CoinCap data into PostgreSQL."""
    processed_data = ti.xcom_pull(task_ids="process_crypto_data", key="processed_data")
    if not processed_data:

        raise ValueError("No processed data available for ingestion")

    try:
        conn = psycopg2.connect(
            host=PG_HOST, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD, port=PG_PORT
        )
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS assets (
                id TEXT,
                rank TEXT,
                symbol TEXT,
                name TEXT,
                price_usd FLOAT,
                timestamp TIMESTAMP, 
                PRIMARY KEY(symbol, timestamp)
            );
        """)
        cursor.executemany(
            "INSERT INTO assets (id, rank, symbol, name, price_usd, timestamp) VALUES (%s, %s, %s, %s, %s, %s);",
            processed_data
        )
        conn.commit()
        cursor.close()
        conn.close()
        print("Successfully ingested data into PostgreSQL.")
    except Exception as e:
        print(f"PostgreSQL ingestion failed: {e}")
        

def upload_to_s3(ti):
    """Save processed data to CSV and upload to S3."""
    processed_data = ti.xcom_pull(task_ids="process_crypto_data", key="processed_data")
    if not processed_data:
        print("No processed data available for CSV export")
        return

    with open(LOCAL_FILE_PATH, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["id", "rank", "symbol", "name", "price_usd", "timestamp"])
        writer.writerows(processed_data)
        

    print(f"CSV saved locally at {LOCAL_FILE_PATH}")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    s3_client.upload_file(LOCAL_FILE_PATH, S3_BUCKET_NAME, S3_FILE_PATH)
    print(f"CSV uploaded to s3://{S3_BUCKET_NAME}/{S3_FILE_PATH}")
    os.remove(LOCAL_FILE_PATH)
    print("Removed staged file")

with DAG(
    "my_dag",
    start_date=datetime(2024, 2, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_crypto_data",
        python_callable=fetch_coincap_data
    )

    process_crypto_data = PythonOperator(
        task_id="process_crypto_data",
        python_callable=process_data
    )

    ingest_data = PythonOperator(
        task_id="ingest_data_to_postgres",
        python_callable=ingest_data_to_postgres
    )

    upload_csv = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        trigger_rule=TriggerRule.ALL_DONE  # Runs even if ingestion fails
    )

    fetch_data >> process_crypto_data >> [ingest_data, upload_csv]
