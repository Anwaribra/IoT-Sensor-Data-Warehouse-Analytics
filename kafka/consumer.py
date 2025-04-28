from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime
from loguru import logger
import os
from dotenv import load_dotenv

load_dotenv()

class IoTTelemetryConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'iot_telemetry',  # Topic name 
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='iot-telemetry-group',
            auto_offset_reset='earliest',
            api_version=(2, 0, 0)
        )
        self.timescale_conn = psycopg2.connect(
            host=os.getenv('TIMESCALEDB_HOST', 'localhost'),
            port=5433,
            database=os.getenv('TIMESCALEDB_DB', 'sensor_data'),
            user=os.getenv('TIMESCALEDB_USER', 'postgres'),
            password=os.getenv('TIMESCALEDB_PASSWORD', 'postgres')
        )
        self.create_tables()

    def create_tables(self):
        """Create necessary tables in TimescaleDB."""
        try:
            with self.timescale_conn.cursor() as cur:
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS iot_telemetry (
                        timestamp TIMESTAMPTZ NOT NULL,
                        device TEXT NOT NULL,
                        co DOUBLE PRECISION,
                        humidity DOUBLE PRECISION,
                        light BOOLEAN,
                        lpg DOUBLE PRECISION,
                        motion BOOLEAN,
                        smoke DOUBLE PRECISION,
                        temp DOUBLE PRECISION
                    );
                """)
                # Create hypertable for time-series data
                cur.execute("""
                    SELECT create_hypertable('iot_telemetry', 'timestamp', 
                        if_not_exists => TRUE);
                """)
                
                # Create index on device for faster queries
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_iot_telemetry_device 
                    ON iot_telemetry (device);
                """)
                
                self.timescale_conn.commit()
                logger.info("TimescaleDB tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            self.timescale_conn.rollback()

    def store_in_timescaledb(self, data):
        """Store telemetry data in TimescaleDB."""
        try:
            with self.timescale_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO iot_telemetry (
                        timestamp, device, co, humidity, light, 
                        lpg, motion, smoke, temp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    datetime.fromisoformat(data['timestamp']),
                    data['device'],
                    data['co'],
                    data['humidity'],
                    data['light'],
                    data['lpg'],
                    data['motion'],
                    data['smoke'],
                    data['temp']
                ))
                self.timescale_conn.commit()
                logger.info(f"Stored telemetry data for device {data['device']}")
        except Exception as e:
            logger.error(f"Error storing in TimescaleDB: {str(e)}")
            self.timescale_conn.rollback()

    def process_message(self, message):
        """Process a single message."""
        try:
            self.store_in_timescaledb(message)
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")

    def run(self):
        """Main consumer loop."""
        logger.info("Starting IoT Telemetry consumer...")
        try:
            for message in self.consumer:
                self.process_message(message.value)
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        finally:
            self.close()

    def close(self):
        """Close all connections."""
        self.consumer.close()
        self.timescale_conn.close()
        logger.info("Consumer connections closed")

if __name__ == "__main__":
    consumer = IoTTelemetryConsumer()
    consumer.run() 