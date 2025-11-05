# airflow/dags/spotify_etl_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from utils.logger import etl_logger
import sys
import os

# Add project modules to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from data_processing.extract.spotify_extract import SpotifyDataExtractor
from data_processing.transform.spotify_transform import SpotifyDataTransformer
from data_processing.load.db_loader import DatabaseLoader
from utils.config import DB_URL


# Default arguments
default_args = {
    'owner': 'spotify_etl',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30)
}

# DAG definition
dag = DAG(
    'spotify_data_etl',
    default_args=default_args,
    description='Extract, transform and load Spotify listening data',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['spotify', 'music', 'etl', 'personal']
)

def check_spotify_credentials(**context):
    """Check if Spotify credentials are available"""
    etl_logger.info("🔐 Checking Spotify credentials...")
    
    try:
        # Check if credentials exist in Airflow Variables
        client_id = Variable.get("SPOTIFY_CLIENT_ID", default_var=None)
        client_secret = Variable.get("SPOTIFY_CLIENT_SECRET", default_var=None)
        
        if not client_id or not client_secret:
            raise AirflowException("Spotify credentials not found in Airflow Variables")
        
        etl_logger.info("✅ Spotify credentials found")
        return {
            'status': 'success',
            'message': 'Credentials verified'
        }
        
    except Exception as e:
        etl_logger.error(f"❌ Spotify credential check failed: {e}")
        raise AirflowException(f"Spotify credential check failed: {e}")

def extract_spotify_data(**context):
    """Extract data from Spotify API - mirrors your test function"""
    etl_logger.info("📥 Starting Spotify data extraction...")
    
    try:
        # Initialize extractor (same as your test)
        extractor = SpotifyDataExtractor()
        
        # Extract data (same as your test)
        raw_data = extractor.extract_all_data()
        
        # Basic validation (same as your test assertions)
        if not all(key in raw_data for key in ['profile', 'top_tracks', 'top_artists']):
            raise AirflowException("Missing required data from Spotify API")
        
        # Log extraction statistics
        stats = {
            'top_tracks': len(raw_data.get('top_tracks', [])),
            'top_artists': len(raw_data.get('top_artists', [])),
            'recently_played': len(raw_data.get('recently_played', [])),
            'saved_tracks': len(raw_data.get('saved_tracks', []))
        }
        
        etl_logger.info(f"✅ Extraction completed: {stats}")
        
        # Push raw data to XCom for next task
        context['ti'].xcom_push(key='raw_spotify_data', value=raw_data)
        
        return {
            'status': 'success',
            'records_extracted': stats
        }
        
    except Exception as e:
        etl_logger.error(f"❌ Extraction failed: {e}")
        raise AirflowException(f"Spotify data extraction failed: {e}")

def transform_spotify_data(**context):
    """Transform raw Spotify data into structured format - mirrors your test function"""
    etl_logger.info("🔄 Starting data transformation...")
    
    try:
        # Get raw data from previous task
        ti = context['ti']
        raw_data = ti.xcom_pull(task_ids='extract_spotify_data', key='raw_spotify_data')
        
        if not raw_data:
            raise AirflowException("No raw data found from extraction task")
        
        # Transform data (same as your test)
        transformer = SpotifyDataTransformer()
        transformed_data = transformer.transform_all_data(raw_data)
        
        # Basic validation (same as your test assertions)
        required_keys = ['artists', 'top_tracks', 'top_artists', 'listening_history']
        if not all(key in transformed_data for key in required_keys):
            raise AirflowException("Missing required transformed data")
        
        # Log transformation statistics
        stats = {
            'artists': len(transformed_data.get('artists', [])),
            'top_tracks': len(transformed_data.get('top_tracks', [])),
            'top_artists': len(transformed_data.get('top_artists', [])),
            'listening_history': len(transformed_data.get('listening_history', []))
        }
        
        etl_logger.info(f"✅ Transformation completed: {stats}")
        
        # Push transformed data to XCom for next task
        ti.xcom_push(key='transformed_spotify_data', value=transformed_data)
        
        return {
            'status': 'success',
            'records_transformed': stats
        }
        
    except Exception as e:
        etl_logger.error(f"❌ Transformation failed: {e}")
        raise AirflowException(f"Data transformation failed: {e}")

def load_spotify_data(**context):
    """Load transformed data into database - mirrors your test function"""
    etl_logger.info("📤 Starting data loading...")
    
    try:
        # Get transformed data from previous task
        ti = context['ti']
        transformed_data = ti.xcom_pull(task_ids='transform_spotify_data', key='transformed_spotify_data')
        
        if not transformed_data:
            raise AirflowException("No transformed data found from transformation task")
        
        # Load data into database (same as your test)
        loader = DatabaseLoader(db_url=DB_URL)
        loader.load_spotify_data(transformed_data)
        
        # Log loading statistics
        stats = {
            'artists_loaded': len(transformed_data.get('artists', [])),
            'tracks_loaded': len(transformed_data.get('top_tracks', [])),
            'artist_rankings_loaded': len(transformed_data.get('top_artists', [])),
            'history_loaded': len(transformed_data.get('listening_history', []))
        }
        
        etl_logger.info(f"✅ Data loading completed: {stats}")
        
        return {
            'status': 'success',
            'records_loaded': stats
        }
        
    except Exception as e:
        etl_logger.error(f"❌ Data loading failed: {e}")
        raise AirflowException(f"Data loading failed: {e}")

def validate_etl_results(**context):
    """Validate the ETL process completed successfully"""
    etl_logger.info("✅ Validating ETL results...")
    
    try:
        ti = context['ti']
        
        # Get results from all tasks
        extraction_result = ti.xcom_pull(task_ids='extract_spotify_data')
        transformation_result = ti.xcom_pull(task_ids='transform_spotify_data')
        loading_result = ti.xcom_pull(task_ids='load_spotify_data')
        
        # Validate all steps were successful
        if not all([
            extraction_result and extraction_result.get('status') == 'success',
            transformation_result and transformation_result.get('status') == 'success',
            loading_result and loading_result.get('status') == 'success'
        ]):
            raise AirflowException("One or more ETL steps failed")
        
        # Compile final statistics
        final_stats = {
            'extraction': extraction_result.get('records_extracted', {}),
            'transformation': transformation_result.get('records_transformed', {}),
            'loading': loading_result.get('records_loaded', {})
        }
        
        etl_logger.info("🎉 ETL pipeline completed successfully!")
        etl_logger.info(f"📊 Final Statistics: {final_stats}")
        
        return {
            'status': 'success',
            'final_stats': final_stats,
            'message': 'Spotify ETL pipeline completed successfully'
        }
        
    except Exception as e:
        etl_logger.error(f"❌ ETL validation failed: {e}")
        raise AirflowException(f"ETL validation failed: {e}")

# Define tasks
start_task = DummyOperator(
    task_id='start_etl_pipeline',
    dag=dag,
)

check_credentials_task = PythonOperator(
    task_id='check_spotify_credentials',
    python_callable=check_spotify_credentials,
    dag=dag,
)

extract_data_task = PythonOperator(
    task_id='extract_spotify_data',
    python_callable=extract_spotify_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_spotify_data',
    python_callable=transform_spotify_data,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_spotify_data',
    python_callable=load_spotify_data,
    dag=dag,
)

validate_results_task = PythonOperator(
    task_id='validate_etl_results',
    python_callable=validate_etl_results,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_etl_pipeline',
    dag=dag,
)

# Email notification for failures
email_on_failure = EmailOperator(
    task_id='email_on_failure',
    to='{{ var.value.get("ALERT_EMAIL", "your-email@example.com") }}',
    subject='Spotify ETL Pipeline Failed - {{ ds }}',
    html_content="""
    <h3>❌ Spotify ETL Pipeline Failure</h3>
    <p>The Spotify ETL pipeline failed on <strong>{{ ds }}</strong>.</p>
    <p><strong>Run ID:</strong> {{ run_id }}</p>
    <p><strong>Task:</strong> {{ task_instance.task_id }}</p>
    <p>Please check the Airflow logs for details.</p>
    """,
    dag=dag,
    trigger_rule='one_failed'
)

# Email notification for success
email_on_success = EmailOperator(
    task_id='email_on_success',
    to='{{ var.value.get("ALERT_EMAIL", "your-email@example.com") }}',
    subject='Spotify ETL Pipeline Succeeded - {{ ds }}',
    html_content="""
    <h3>✅ Spotify ETL Pipeline Success</h3>
    <p>The Spotify ETL pipeline completed successfully on <strong>{{ ds }}</strong>.</p>
    <p><strong>Run ID:</strong> {{ run_id }}</p>
    <p><strong>Summary:</strong></p>
    <ul>
        <li>Artists processed: {{ ti.xcom_pull(task_ids='validate_etl_results')['final_stats']['loading']['artists_loaded'] }}</li>
        <li>Tracks processed: {{ ti.xcom_pull(task_ids='validate_etl_results')['final_stats']['loading']['tracks_loaded'] }}</li>
        <li>Listening history: {{ ti.xcom_pull(task_ids='validate_etl_results')['final_stats']['loading']['history_loaded'] }}</li>
    </ul>
    <p>Data is now available in your dashboard.</p>
    """,
    dag=dag,
    trigger_rule='all_success'
)

# Define task dependencies
start_task >> check_credentials_task >> extract_data_task
extract_data_task >> transform_data_task >> load_data_task
load_data_task >> validate_results_task >> [email_on_success, email_on_failure] >> end_task