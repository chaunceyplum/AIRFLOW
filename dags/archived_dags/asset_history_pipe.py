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
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
dt = datetime.today()  # Get timezone naive now
seconds = dt.timestamp()

# PostgreSQL connection details
PG_HOST = Variable.get("PG_HOST")
# PG_DATABASE = Variable.get("PG_DATABASE")
PG_DATABASE = "crypto"
PG_USER = Variable.get("PG_USER")
PG_PASSWORD = Variable.get("PG_PASSWORD")
PG_PORT = 5432
PG_URL = Variable.get("PG_URL")
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

hook = PostgresHook(postgres_conn_id='postgres')

def fetch_assets(ti):
   df = hook.get_pandas_df(sql="select symbol from assets group by symbol;")
   # count = hook.get_pandas_df(sql="select count from assets;")
   
   
   ti.xcom_push(key="asset_data", value=df)
def process_assets(ti):
    asset_symbols = ti.xcom_pull(task_ids="fetch_assets",key="asset_data" )

    symbols = asset_symbols
    arr = []
    for symbol in symbols:
      try:
       # index = 1 + index
       obj = {
           "symbol":symbols
       }
       arr.append(obj)
      except:
         print("unable to loop through data")
       





    ti.xcom_push(key="asset_symbols",value=arr) 

with DAG(
    "asset_history_pipe",
    start_date=datetime(2024, 2, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:


    # fetch_assets = PostgresOperator(
    #     task_id="load_assets",
    #     postgres_conn_id="postgres",
    #     sql="{{ task_instance.xcom_pull(task_ids='process_data', key='asset_sql_query') }}",
    # )
    fetch_asset = PythonOperator(
        task_id="fetch_assets",
        python_callable=fetch_assets
    )

    process_asset = PythonOperator(
        task_id="process_assets",
        python_callable=process_assets
    )

    # upload_csv = PythonOperator(
    #     task_id="upload_to_s3",
    #     python_callable=upload_to_s3,
    #     trigger_rule=TriggerRule.ALL_DONE  # Runs even if ingestion fails
    # )

    # fetch_data >> process_crypto_data >> [ingest_data, upload_csv]
    fetch_asset >> process_asset
