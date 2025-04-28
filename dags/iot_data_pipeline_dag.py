from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sys
import os
import pandas as pd
from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from kafka.producer import IoTTelemetryProducer
from kafka.consumer import IoTTelemetryConsumer

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
    tags=['iot', 'data'],
)

def check_data_quality(**context):
    """Check data quality of IoT telemetry data."""
    try:
        # Read the CSV file
        df = pd.read_csv("data/iot_telemetry_data.csv")
        
        # Check for missing values
        missing_values = df.isnull().sum()
        if missing_values.any():
            logger.warning(f"Missing values found: {missing_values[missing_values > 0]}")
        
        # Check data types
        expected_types = {
            'ts': float,
            'device': str,
            'co': float,
            'humidity': float,
            'light': bool,
            'lpg': float,
            'motion': bool,
            'smoke': float,
            'temp': float
        }
        
        for col, expected_type in expected_types.items():
            if not pd.api.types.is_dtype_equal(df[col].dtype, expected_type):
                logger.warning(f"Column {col} has incorrect type: {df[col].dtype}, expected {expected_type}")
        
        # Check value ranges
        if (df['co'] < 0).any():
            logger.warning("Negative CO values found")
        if (df['humidity'] < 0).any() or (df['humidity'] > 100).any():
            logger.warning("Humidity values outside valid range (0-100)")
        if (df['temp'] < -40).any() or (df['temp'] > 100).any():
            logger.warning("Temperature values outside valid range (-40 to 100°C)")
        
        logger.info("Data quality checks completed successfully")
        return True
    except Exception as e:
        logger.error(f"Data quality check failed: {str(e)}")
        raise

def run_kafka_pipeline(**context):
    """Run the Kafka producer and consumer pipeline."""
    try:
        # Start the producer
        producer = IoTTelemetryProducer()
        producer.run(batch_size=100, interval=1.0)
        
        # Start the consumer
        consumer = IoTTelemetryConsumer()
        consumer.run()
        
        logger.info("Kafka pipeline completed successfully")
        return True
    except Exception as e:
        logger.error(f"Kafka pipeline failed: {str(e)}")
        raise

# Define tasks
check_quality = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag,
)

run_pipeline = PythonOperator(
    task_id='run_kafka_pipeline',
    python_callable=run_kafka_pipeline,
    dag=dag,
)

# Set task dependencies
check_quality >> run_pipeline 