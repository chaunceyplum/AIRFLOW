from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Connection
from airflow.settings import Session
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
def sanitize_value(value):
    """Convert empty values to NULL for SQL compatibility"""
    if value in [None, ""]:
        return "NULL"
    return f"'{value}'"  # Ensure proper SQL string formatting

def create_airflow_postgres_connection():
    """Creates a PostgreSQL connection in Airflow dynamically."""
    session = Session()
    conn_id = "postgres"

    # Check if connection exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    
    if not existing_conn:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type="postgres",
            host=PG_HOST,
            # schema="public",
            login=PG_USER,
            password=PG_PASSWORD,
            port=5432
        )
        session.add(new_conn)
        session.commit()
        print(f"Created new Airflow connection: {conn_id}")
    else:
        print(f"Connection {conn_id} already exists")

    session.close()

# Call the function to create the connection
create_airflow_postgres_connection()




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

    time = datetime.now().isoformat()

    processed_asset_data = [
        (sanitize_value(item.get("id")),
            sanitize_value(item.get("rank")),
            sanitize_value(item.get("symbol")),
            sanitize_value(item.get("name")),
            sanitize_value(item.get("priceUsd")),
            sanitize_value(item.get("supply")),
            sanitize_value(item.get("maxSupply")),
            sanitize_value(item.get("marketCapUsd")),
            sanitize_value(item.get("volumeUsd24Hr")),
            sanitize_value(item.get("changePercent24Hr")),
            sanitize_value(item.get("vwap24Hr")),
        #  item[time]
         )
        for item in asset_data
    ]
    processed_market_data = [
        (
        item["exchangeId"], 
         item["rank"],
         item["baseSymbol"],
         item["baseId"],
         item["quoteSymbol"],
         item["quoteId"],  
         item["priceQuote"],
         item["priceUsd"],
         item["volumeUsd24Hr"],
         item["percentExchangeVolume"],
         item["tradesCount24Hr"],
        #  datetime.now().isoformat()
         )
        for item in market_data
    ]    
    processed_rate_data = [
        (item["id"], 
         item["currencySymbol"], 
         item["symbol"], 
         item["type"], 
         item["rateUsd"]
        #  datetime.now().isoformat()
         )
        for item in rate_data
    ]
    processed_exchange_data = [
        (item["exchangeId"], 
         item["name"],
         item["rank"],
         item["percentTotalVolume"],
         item["volumeUsd"],
         item["tradingPairs"], 
         item["socket"], 
         item["exchangeUrl"] 
        #  datetime.now().isoformat()
         )
        for item in exchange_data
    ]

    asset_sql_query = "INSERT INTO assets (id, rank, symbol, name, priceUsd, supply, maxSupply, marketCapUsd, volumeUsd24Hr, changePercent24Hr, vwap24Hr) VALUES " + \
                      ", ".join([str(tuple(row)).replace("'NULL'", "NULL") for row in processed_asset_data]) + ";"
    ti.xcom_push(key='asset_sql_query', value=asset_sql_query)

    market_sql_query = "INSERT INTO markets (exchangeId, rank, baseSymbol, baseId, quoteSymbol, quoteId, priceQuote, priceUsd, volumeUsd24Hr, percentExchangeVolume ,tradesCount24Hr) VALUES " + \
                       ", ".join([str(tuple(row)).replace("'NULL'", "NULL") for row in processed_market_data]) + ";"
    ti.xcom_push(key='market_sql_query', value=market_sql_query)

    rate_sql_query = "INSERT INTO rates (id, currencySymbol, symbol, type, rateUsd) VALUES " + \
                     ", ".join([str(tuple(row)).replace("'NULL'", "NULL") for row in processed_rate_data]) + ";"
    ti.xcom_push(key='rate_sql_query', value=rate_sql_query)

    exchange_sql_query = "INSERT INTO exchanges (exchangeId, name, rank, percentTotalVolume, volumeUsd, tradingPairs, socket, exchangeUrl) VALUES " + \
                         ", ".join([str(tuple(row)).replace("'NULL'", "NULL") for row in processed_exchange_data]) + ";"
    ti.xcom_push(key='exchange_sql_query', value=exchange_sql_query)
    # ti.xcom_push( key="asset_data", value=processed_assets_data)
    # ti.xcom_push(key="fetch_market_data", value=processed_market_data)
    # ti.xcom_push(key="fetch_rate_data", value=processed_rate_data)
    # ti.xcom_push(key="fetch_exchange_data", value=processed_exchange_data)    






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

    load_assets = PostgresOperator(
        task_id="load_assets",
        postgres_conn_id="postgres",
        sql="{{ task_instance.xcom_pull(task_ids='process_data', key='asset_sql_query') }}",
    )




    [fetch_assets_data, fetch_rates_data, fetch_exchanges_data, fetch_markets_data] >> process_data >> load_assets
