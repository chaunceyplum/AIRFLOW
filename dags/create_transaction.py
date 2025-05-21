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
import uuid
import json

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


with DAG(
    'simulate_store',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Runs every 30 minutes
    catchup=False,
) as dag:

    

    grab_cust_data = PythonOperator(
        task_id='insert_customers',
        python_callable=grab_customer_data,
    )




    grab_customer_data 
