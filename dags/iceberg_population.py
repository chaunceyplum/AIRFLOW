from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import boto3
import pandas as pd
import psycopg2
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema
from pyiceberg.types import *
from pyiceberg.table import Table
import pyarrow as pa
import logging
import os

# ───── Airflow Config ───── #
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
}
logging.basicConfig(level=logging.INFO)

# ───── Airflow Variables ───── #
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-1"

S3_WAREHOUSE_PATH = "s3://baisleylake/rds/Iceberg_lakehouse/"

PG_HOST = Variable.get("PG_HOST")
PG_PORT = Variable.get("PG_PORT")
PG_DB = Variable.get("PG_DATABASE")
PG_USER = Variable.get("PG_USER")
PG_PASSWORD = Variable.get("PG_PASSWORD")

TABLE_SCHEMAS = {
    "person": Schema(
        NestedField(1, "person_id", StringType(), True),
        NestedField(2, "first_name", StringType(), False),
        NestedField(3, "last_name", StringType(), False),
        NestedField(4, "email", StringType(), False),
        NestedField(5, "created_at", TimestampType(), False),
    ),
    "product": Schema(
        NestedField(1, "product_id", StringType(), True),
        NestedField(2, "product_name", StringType(), False),
        NestedField(3, "price", DoubleType(), False),
        NestedField(4, "in_stock", BooleanType(), False),
    ),
    "orderitem": Schema(
        NestedField(1, "order_id", StringType(), True),
        NestedField(2, "fk_person_id", StringType(), False),
        NestedField(3, "fk_product_id", StringType(), False),
        NestedField(4, "fk_transaction_id", StringType(), False),
        NestedField(5, "number_of_products", IntegerType(), False),
        NestedField(6, "price", DoubleType(), False),
        NestedField(7, "created_at", TimestampType(), False),
    ),
    "transactions": Schema(
        NestedField(1, "transaction_id", StringType(), True),
        NestedField(2, "fk_person_id", StringType(), False),
        # NestedField(3, "fk_order_id", StringType(), False),
        NestedField(3, "total_price", DoubleType(), False),
        NestedField(4, "created_at", TimestampType(), False),
    ),
}

os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY

def iceberg_type_to_pyarrow_type(iceberg_type):
    if isinstance(iceberg_type, StringType):
        return pa.string()
    elif isinstance(iceberg_type, IntegerType):
        return pa.int32()
    elif isinstance(iceberg_type, LongType):
        return pa.int64()
    elif isinstance(iceberg_type, DoubleType):
        return pa.float64()
    elif isinstance(iceberg_type, BooleanType):
        return pa.bool_()
    elif isinstance(iceberg_type, TimestampType):
        return pa.timestamp("us")
    else:
        raise TypeError(f"Unsupported Iceberg type: {iceberg_type}")

def get_catalog():
    logging.info(f"Creating boto3 session with region: {AWS_REGION}")
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    glue_client = session.client('glue')

    try:
        response = glue_client.get_databases(MaxResults=1)
        logging.info(f"Glue client connected. Sample databases: {[db['Name'] for db in response['DatabaseList']]}")
    except Exception as e:
        logging.error(f"Failed to connect to Glue: {e}")
        raise e

    catalog = GlueCatalog(
        name="IcebergCatalog",
        warehouse=S3_WAREHOUSE_PATH,
        glue=glue_client,
    )
    return catalog

def ingest_table_to_iceberg(table_name: str, **kwargs):
    logging.info(f"Ingesting {table_name} to Iceberg")

    conn = psycopg2.connect(
        host=PG_HOST,
        port="5432",
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()

    catalog = get_catalog()
    table_id = f"postgres.{table_name}"

    if not catalog.table_exists(table_id):
        schema = TABLE_SCHEMAS[table_name]
        catalog.create_table(identifier=table_id, schema=schema)
        logging.info(f"Created Iceberg table: {table_id}")
    else:
        logging.info(f"Iceberg table {table_id} already exists")

    iceberg_schema = TABLE_SCHEMAS[table_name]
    required_fields = [f for f in iceberg_schema.fields if f.required]

    for field in required_fields:
        if df[field.name].isnull().any():
            raise ValueError(f"Required field '{field.name}' contains nulls")

    arrow_fields = []
    for f in iceberg_schema.fields:
        pa_type = iceberg_type_to_pyarrow_type(f.field_type)
        arrow_fields.append(pa.field(f.name, pa_type, nullable=not f.required))
    enforced_schema = pa.schema(arrow_fields)

    arrow_table = pa.Table.from_pandas(df, schema=enforced_schema, preserve_index=False)

    iceberg_table = catalog.load_table(table_id)
    iceberg_table.append(arrow_table)

    logging.info(f"Appended {len(df)} rows to {table_name}")

with DAG(
    dag_id='postgres_to_iceberg_daily',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    table_names = list(TABLE_SCHEMAS.keys())

    for table in table_names:
        PythonOperator(
            task_id=f"ingest_{table}_to_iceberg",
            python_callable=ingest_table_to_iceberg,
            op_kwargs={"table_name": table},
        )
