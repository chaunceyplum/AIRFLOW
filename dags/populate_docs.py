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
import logging
from io import StringIO

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

conn = psycopg2.connect(f"dbname='{PG_DATABASE}' user='{PG_USER}' host='{PG_HOST}' password='{PG_PASSWORD}'")
cur = conn.cursor()

def grab_customer_data(ti):
    # Execute the query
    cur.execute('SELECT * FROM person')
    # Get column names
    colnames = [desc[0] for desc in cur.description]
    # Fetch all rows
    rows = cur.fetchall()
    # Create DataFrame
    person_df = pd.DataFrame(rows, columns=colnames)
    print(person_df.head())

    ti.xcom_push(key="person_df", value=person_df)


def grab_product_data(ti):
    url = "https://api.mockaroo.com/api/f3aae820?count=1000&key=ab78c110"

    try:
        response = requests.get(url)
        response.raise_for_status()

        logging.info(f"Status Code: {response.status_code}")
        logging.info(f"Response Length: {len(response.text)}")
        logging.info(f"Content-Type: {response.headers.get('Content-Type', '')}")

        # Read CSV from response
        csv_text = response.text
        product_df = pd.read_csv(StringIO(csv_text))

        logging.info("First few rows of product_df:")
        logging.info(product_df.head().to_string())

        # Push to XCom as JSON-safe object
        ti.xcom_push(key="product_df", value=product_df.to_dict(orient='records'))
        # products = product_df.to_dict(orient='records')
        # for tx in products:
        #     cur.execute("""
        #         INSERT INTO products (product_id, product_name, product_description, product_amount, created_at)
        #         VALUES (%s, %s, %s, %s, %s, )
        #         ON CONFLICT (product_id) DO NOTHING;
        #     """, (
        #         tx['fk_transaction_id'],
        #         tx['created_at'],
        #         tx['fk_product_id'],
        #         tx['order_id'],
        #         tx['quantity'],
        #         tx['unit_price']
        #     ))

    except requests.RequestException as e:
        logging.error(f"Request to {url} failed: {e}")
        raise e

def merge_data(ti):

    product_data = ti.xcom_pull(task_ids='grab_product_data', key='product_df')
    product_df = pd.DataFrame(product_data)
    person_df = ti.xcom_pull(key='person_df', task_ids='grab_customer_data')

    transactions = []
    order_items = []

    for _, person in person_df.iterrows():
        num_transactions = random.randint(1, 3)

        for _ in range(num_transactions):
            transaction_id = str(uuid.uuid4())
            transaction_time = (datetime.utcnow() - timedelta(days=random.randint(0, 30))).isoformat()
            sampled_products = product_df.sample(n=random.randint(1, min(3, len(product_df))))
            total_price = 0.0

            for _, product in sampled_products.iterrows():
                quantity = random.randint(1, 5)
                unit_price = float(product['product_amount'])
                item_total = quantity * unit_price

                order_item = {
                    "order_id": str(uuid.uuid4()),
                    "quantity": quantity,
                    "fk_product_id": product["product_id"],
                    "created_at": transaction_time,
                    "fk_transaction_id": transaction_id,
                    "unit_price": unit_price,
                }
                order_items.append(order_item)
                total_price += item_total

            transaction = {
                "transaction_id": transaction_id,
                "created_at": transaction_time,
                "fk_person_id": person["person_id"],
                "total_price": round(total_price, 2),
            }
            transactions.append(transaction)

    ti.xcom_push(key="transactions", value=transactions)
    ti.xcom_push(key="order_items", value=order_items)

    return transactions, order_items

def ingest_transactions(ti):
    transactions = ti.xcom_pull(task_ids='merge_customer_and_product_data', key='transactions')

    for tx in transactions:
        cur.execute("""
            INSERT INTO transactions (transaction_id, created_at, fk_person_id, total_price)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING;
        """, (
            tx['transaction_id'],
            tx['created_at'],
            tx['fk_person_id'],
            tx['total_price']
        ))

def ingest_order_items(ti):
    orderItem = ti.xcom_pull(task_ids='merge_customer_and_product_data', key='order_items')

    for tx in orderItem:
        cur.execute("""
            INSERT INTO orderitem (created_at, fk_product_id, fk_transaction_id, order_id, quantity, unit_price)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING;
        """, (
            tx['created_at'],
            tx['fk_product_id'],
            tx['fk_transaction_id'],
            tx['order_id'],
            tx['quantity'],
            tx['unit_price']
        ))


with DAG(
    'populate_oltp',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    grab_cust_data = PythonOperator(
        task_id='grab_customer_data',
        python_callable=grab_customer_data,
    )

    grab_prod_data = PythonOperator(
        task_id='grab_product_data',
        python_callable=grab_product_data,
    )

    merge_task = PythonOperator(
        task_id='merge_customer_and_product_data',
        python_callable=merge_data,
    )

    ingest_transaction = PythonOperator(
        task_id='ingest_transactions',
        python_callable=ingest_transactions
    )

    ingest_order_item = PythonOperator(
        task_id='ingest_order_items',
        python_callable=ingest_order_items
    )

    [grab_cust_data, grab_prod_data] >> merge_task >> [ingest_transaction, ingest_order_item]
