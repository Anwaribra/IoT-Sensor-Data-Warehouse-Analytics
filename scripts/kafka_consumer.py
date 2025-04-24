from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime
from loguru import logger
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class SensorDataConsumer:
    def __init__(self):
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            os.getenv('KAFKA_TOPIC', 'sensor-data'),
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id=os.getenv('KAFKA_CONSUMER_GROUP', 'sensor-data-group'),
            auto_offset_reset='earliest'
        )

        # Initialize TimescaleDB connection
        self.timescale_conn = psycopg2.connect(
            host=os.getenv('TIMESCALEDB_HOST', 'localhost'),
            port=os.getenv('TIMESCALEDB_PORT', 5432),
            database=os.getenv('TIMESCALEDB_DB', 'sensor_data'),
            user=os.getenv('TIMESCALEDB_USER', 'postgres'),
            password=os.getenv('TIMESCALEDB_PASSWORD', 'postgres')
        )
        self.create_tables()

    def create_tables(self):
        """Create necessary tables in TimescaleDB."""
        try:
            with self.timescale_conn.cursor() as cur:
                # Create sensor_data table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS sensor_data (
                        time TIMESTAMPTZ NOT NULL,
                        sensor_id TEXT NOT NULL,
                        value DOUBLE PRECISION,
                        metadata JSONB
                    );
                """)
                
                # Convert to hypertable
                cur.execute("""
                    SELECT create_hypertable('sensor_data', 'time', 
                        if_not_exists => TRUE);
                """)
                
                self.timescale_conn.commit()
                logger.info("TimescaleDB tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            self.timescale_conn.rollback()

    def store_in_timescaledb(self, data):
        """Store data in TimescaleDB."""
        try:
            with self.timescale_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO sensor_data (time, sensor_id, value, metadata)
                    VALUES (%s, %s, %s, %s)
                """, (
                    datetime.fromisoformat(data['timestamp']),
                    data['sensor_id'],
                    float(data['value']),
                    json.dumps(data.get('metadata', {}))
                ))
                self.timescale_conn.commit()
                logger.info(f"Stored data in TimescaleDB for sensor {data['sensor_id']}")
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
        logger.info("Starting Kafka consumer...")
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

if __name__ == "__main__":
    consumer = SensorDataConsumer()
    consumer.run() 