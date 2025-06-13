from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import os
import logging
from airflow.models import Variable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = 'heroku_postgres'

PG_HOST = Variable.get("PG_HOST")
PG_DATABASE = Variable.get("PG_DATABASE")
PG_USER = Variable.get("PG_USER")
PG_PASSWORD = Variable.get("PG_PASSWORD")
PG_PORT = 5432



# Set default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to scrape NBA player stats
def scrape_nba_player_stats():
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    url = f'https://www.balldontlie.io/api/v1/stats?start_date={yesterday}&end_date={yesterday}'
    logger.info(f"Fetching NBA player stats from {url}")
    response = requests.get(url)
    data = response.json()
    logger.info(f"Fetched {len(data['data'])} player stats")
    return data['data']  # Extract the list of player stats

# Function to ingest player stats into PostgreSQL
def ingest_into_postgres(**kwargs):
    stats = kwargs['ti'].xcom_pull(task_ids='scrape_nba_player_stats')
    if not stats:
        logger.info("No player stats found for yesterday")
        return 'No player stats found for yesterday'
    
    conn = psycopg2.connect(f"dbname='{PG_DATABASE}' user='{PG_USER}' host='{PG_HOST}' password='{PG_PASSWORD}'")
    cur = conn.cursor()
    
    insert_query = '''
        INSERT INTO nba_player_stats (player_id, player_name, team_name, points, assists, rebounds, date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (player_id, date) DO NOTHING;
    '''
    
    for stat in stats:
        player = stat['player']
        team = stat['team']
        logger.info(f"Inserting stats for {player['first_name']} {player['last_name']} - {team['full_name']}")
        cur.execute(insert_query, (
            player['id'],
            f"{player['first_name']} {player['last_name']}",
            team['full_name'],
            stat['pts'],
            stat['ast'],
            stat['reb'],
            # yesterday
        ))
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Player stats ingestion completed successfully")

# Define DAG
with DAG(
    'nba_player_stats_ingestion',
    default_args=default_args,
    description='Scrape yesterdays NBA player stats and ingest into PostgreSQL',
    start_date=datetime(2024, 3, 1),
    schedule_interval="@daily",
    catchup=False,
    ) as dag:

 # Define tasks
 scrape_task = PythonOperator(
     task_id='scrape_nba_player_stats',
     python_callable=scrape_nba_player_stats
 )

 ingest_task = PythonOperator(
     task_id='ingest_into_postgres',
     python_callable=ingest_into_postgres,
     provide_context=True
     
 )

# Set task dependencies
scrape_task >> ingest_task
