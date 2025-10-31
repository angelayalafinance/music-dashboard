# airflow/dags/spotify_data_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
import logging

# Add your project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from extract.spotify_extract import SpotifyDataExtractor
from transform.spotify_transform import DataTransformer
from load.database_loader import DatabaseLoader

default_args = {
    'owner': 'spotify_user',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_spotify_data(**context):
    """Task to extract data from Spotify"""
    try:
        extractor = SpotifyDataExtractor()
        raw_data = extractor.extract_all_data()
        
        # Push data to XCom for next task
        context['ti'].xcom_push(key='raw_spotify_data', value=raw_data)
        logging.info(f"Extracted {len(raw_data.get('top_tracks', []))} top tracks")
        
        return True
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        raise

def transform_spotify_data(**context):
    """Task to transform Spotify data"""
    try:
        ti = context['ti']
        raw_data = ti.xcom_pull(task_ids='extract_data', key='raw_spotify_data')
        
        transformer = DataTransformer()
        transformed_data = transformer.transform_spotify_data(raw_data)
        
        # Push transformed data to XCom
        ti.xcom_push(key='transformed_spotify_data', value=transformed_data)
        logging.info("Data transformation completed")
        
        return True
    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        raise

def load_spotify_data(**context):
    """Task to load data to database"""
    try:
        ti = context['ti']
        transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_spotify_data')
        
        loader = DatabaseLoader()
        loader.load_spotify_data(transformed_data)
        
        logging.info("Data loading completed")
        return True
    except Exception as e:
        logging.error(f"Loading failed: {e}")
        raise

# Define the DAG
dag = DAG(
    'spotify_data_pipeline',
    default_args=default_args,
    description='Extract and load Spotify user data',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['spotify', 'music', 'personal']
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_spotify_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_spotify_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_spotify_data,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task