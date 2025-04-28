from kafka import KafkaProducer
import json
import time
import pandas as pd
from typing import Dict, Any
from datetime import datetime
from loguru import logger
import os
from dotenv import load_dotenv

load_dotenv()

class IoTTelemetryProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            api_version=(2, 0, 0)
        )
        logger.info("Kafka producer initialized")
        
        self.data_file = "data/iot_telemetry_data.csv"
        self.df = pd.read_csv(self.data_file)
        self.df['ts'] = pd.to_datetime(self.df['ts'].astype(float), unit='s')
        logger.info(f"Loaded {len(self.df)} records from {self.data_file}")

    def publish_telemetry_data(self, row: pd.Series):
        """Publish telemetry data to Kafka topic."""
        try:
            topic = "iot_telemetry"
            message = {
                'timestamp': row['ts'].isoformat(),
                'device': row['device'],
                'co': float(row['co']),
                'humidity': float(row['humidity']),
                'light': bool(row['light']),
                'lpg': float(row['lpg']),
                'motion': bool(row['motion']),
                'smoke': float(row['smoke']),
                'temp': float(row['temp'])
            }
            self.producer.send(topic, value=message)
            self.producer.flush()
            logger.info(f"Published data for device {row['device']}")
        except Exception as e:
            logger.error(f"Error publishing data: {str(e)}")

    def run(self, batch_size: int = 100, interval: float = 1.0):
        """Main producer loop."""
        total_rows = len(self.df)
        current_index = 0
        
        while current_index < total_rows:
            # Process data in batches
            end_index = min(current_index + batch_size, total_rows)
            batch = self.df.iloc[current_index:end_index]
            
            for _, row in batch.iterrows():
                self.publish_telemetry_data(row)
            
            current_index = end_index
            logger.info(f"Processed {current_index}/{total_rows} records")
            
            if current_index < total_rows:
                time.sleep(interval)

    def close(self):
        """Close Kafka producer."""
        self.producer.close()
        logger.info("Kafka producer closed")

if __name__ == "__main__":
    producer = IoTTelemetryProducer()
    try:
        # Process 100 records at a time with 1 second interval
        producer.run(batch_size=100, interval=1.0)
    except KeyboardInterrupt:
        producer.close() 