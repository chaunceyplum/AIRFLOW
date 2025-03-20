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
import numpy as np

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

def fetch_assets(ti):
 url = "https://api.coincap.io/v2/assets?limit=2000"
 response = requests.get(url)
 response.raise_for_status()
 data = response.json()["data"]
 ti.xcom_push(key="asset_data", value=data)

def fetch_rates(ti):
 url = "https://api.coincap.io/v2/rates"
 response = requests.get(url)
 response.raise_for_status()
 data = response.json()["data"]
 ti.xcom_push(key="rate_data", value=data) 


def fetch_markets(ti):
 url = "https://api.coincap.io/v2/markets"
 response = requests.get(url)
 response.raise_for_status()
 data = response.json()["data"]
 ti.xcom_push(key="market_data", value=data) 

def fetch_exchanges(ti):
 url = "https://api.coincap.io/v2/exchanges"
 response = requests.get(url)
 response.raise_for_status()
 data = response.json()["data"]
 ti.xcom_push(key="exchange_data", value=data) 


def data_process(ti):
    asset_data = ti.xcom_pull(task_ids="fetch_asset_data", key="asset_data")
    market_data = ti.xcom_pull(task_ids="fetch_market_data", key="market_data")
    rate_data = ti.xcom_pull(task_ids="fetch_rate_data", key="rate_data")
    exchange_data = ti.xcom_pull(task_ids="fetch_exchange_data", key="exchange_data")

    if not asset_data:
        raise ValueError("No data received from CoinCap API")
    if not market_data:
        raise ValueError("No data received from CoinCap API")
    if not rate_data:
        raise ValueError("No data received from CoinCap API")
    if not exchange_data:
        raise ValueError("No data received from CoinCap API")

    processed_assets_data = [
        (item["id"], item["rank"], item["symbol"], item["name"], float(item["priceUsd"]),float(item["supply"]),float(item["maxSupply"]),float(item["marketCapUsd"]),float(item["volumeUsd24Hr"]),float(item["changePercent24Hr"]),float(item["vwap24Hr"]), datetime.now())
        for item in asset_data
    ]
    processed_market_data = [
        (item["exchangeId"], item["rank"],item["baseSymbol"],item["baseId"],item["quoteSymbol"],item["quoteId"],  float(item["priceQuote"]),float(item["priceUsd"]),float(item["volumeUsd24"]),float(item["percentExchangeVolume"]),float(item["tradesCount24Hr"]), datetime.now())
        for item in market_data
    ]    
    processed_rate_data = [
        (item["id"], item["currencySymbol"], item["symbol"], item["type"], float(item["priceUsd"]), datetime.now())
        for item in rate_data
    ]
    processed_exchange_data = [
        (item["id"], item["name"],float(item["percentTotalVolume"]),float(item["volumeUsd"]),float(item["tradingPairs"]), item["socket"], item["exchangeUrl"], datetime.now())
        for item in exchange_data
    ]


    ti.xcom_push( key="asset_data", value=processed_assets_data)
    ti.xcom_push(key="fetch_market_data", value=processed_market_data)
    ti.xcom_push(key="fetch_rate_data", value=processed_rate_data)
    ti.xcom_push(key="fetch_exchange_data", value=processed_exchange_data)    






# def upload_to_s3(ti):
#     """Save processed data to CSV and upload to S3."""
#     processed_data = ti.xcom_pull(task_ids="fetch_crypto_data", key="coincap_data")
#     if not processed_data:
#         print("No processed data available for CSV export")
#         return

#     with open(LOCAL_FILE_PATH, mode='w', newline='') as file:
#         writer = csv.writer(file)
#         writer.writerow(["id", "rank", "symbol", "name", "price_usd", "timestamp"])
#         writer.writerows(processed_data)

#     print(f"CSV saved locally at {LOCAL_FILE_PATH}")

#     s3_client = boto3.client(
#         "s3",
#         aws_access_key_id=AWS_ACCESS_KEY_ID,
#         aws_secret_access_key=AWS_SECRET_ACCESS_KEY
#     )
#     s3_client.upload_file(LOCAL_FILE_PATH, S3_BUCKET_NAME, S3_FILE_PATH)
#     print(f"CSV uploaded to s3://{S3_BUCKET_NAME}/{S3_FILE_PATH}")
#     os.remove(LOCAL_FILE_PATH)
#     print("Removed staged file")

with DAG(
    "crypto_pipe_1",
    start_date=datetime(2024, 2, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    fetch_assets_data = PythonOperator(
        task_id="fetch_asset_data",
        python_callable=fetch_assets
    )
    fetch_rates_data = PythonOperator(
        task_id="fetch_rate_data",
        python_callable=fetch_rates
    )
    fetch_markets_data = PythonOperator(
        task_id="fetch_market_data",
        python_callable=fetch_markets
    )
    fetch_exchanges_data = PythonOperator(
        task_id="fetch_exchange_data",
        python_callable=fetch_exchanges
    )    
    process_data = PythonOperator(
       task_id='process_data',
       python_callable=data_process
    )




    [fetch_assets_data, fetch_rates_data, fetch_exchanges_data, fetch_markets_data] >> process_data 
