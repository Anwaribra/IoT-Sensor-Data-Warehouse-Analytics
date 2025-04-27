from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sys
import os

# Add the parent directory to the path so we can import the data_pipeline module
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.data_pipeline import run_data_pipeline

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'iot_data_pipeline',
    default_args=default_args,
    description='IoT Data Processing Pipeline',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['iot', 'data-engineering'],
)

def check_data_quality(**context):
    """Check data quality using Great Expectations."""
    from great_expectations import DataContext
    
    context = DataContext()
    results = context.run_validation()
    
    if not results.success:
        raise ValueError("Data quality check failed")

def process_data(**context):
    """Run the data processing pipeline."""
    run_data_pipeline()

# Define tasks
check_quality = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

# Set task dependencies
check_quality >> process_data 