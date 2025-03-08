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

dt = datetime.today()  # Get timezone naive now
seconds = dt.timestamp()

# PostgreSQL connection details
PG_HOST = Variable.get("PG_HOST")
PG_DATABASE = Variable.get("PG_DATABASE")
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

def fetch_coincap_v1_data(ti):
 url = "https://api.coincap.io/v2/assets"
 response = requests.get(url)
 response.raise_for_status()
 data = response.json()["data"]
 ti.xcom_push(key="coincap_data", value=data)

def fetch_coingecko_data(ti):
 url = "https://api.coincap.io/v2/assets" #must be adusted
 response = requests.get(url)
 response.raise_for_status()
 data = response.json()["data"]
 ti.xcom_push(key="coingecko_data", value=data)



with DAG(
    "my_dag",
    start_date=datetime(2024, 2, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    fetch_coincap_data = PythonOperator(
        task_id="fetch_crypto_data",
        python_callable=fetch_coincap_v1_data
    )



    fetch_coincap_data 
