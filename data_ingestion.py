import json
import time
from typing import Dict, Any
import requests
from kafka import KafkaProducer, KafkaConsumer
from loguru import logger
from config import settings

class OpenSenseMapIngestion:
    def __init__(self):
        self.api_url = settings.OPENSENSEMAP_API_URL
        self.api_key = settings.OPENSENSEMAP_API_KEY
        self.producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

    def fetch_sensor_data(self, sensor_id: str) -> Dict[str, Any]:
        """Fetch data from OpenSenseMap API for a specific sensor."""
        try:
            headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
            response = requests.get(
                f"{self.api_url}/boxes/{sensor_id}/data",
                headers=headers
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching data for sensor {sensor_id}: {str(e)}")
            return {}

    def produce_to_kafka(self, data: Dict[str, Any]):
        """Produce sensor data to Kafka topic."""
        try:
            self.producer.send(settings.KAFKA_TOPIC, value=data)
            self.producer.flush()
            logger.info(f"Successfully produced data to Kafka topic {settings.KAFKA_TOPIC}")
        except Exception as e:
            logger.error(f"Error producing to Kafka: {str(e)}")

    def run(self, sensor_ids: list, interval: int = 60):
        """Main ingestion loop."""
        while True:
            for sensor_id in sensor_ids:
                data = self.fetch_sensor_data(sensor_id)
                if data:
                    self.produce_to_kafka(data)
            time.sleep(interval)

class KafkaConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            settings.KAFKA_TOPIC,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=settings.KAFKA_CONSUMER_GROUP,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def consume(self):
        """Consume messages from Kafka topic."""
        try:
            for message in self.consumer:
                yield message.value
        except Exception as e:
            logger.error(f"Error consuming from Kafka: {str(e)}")

if __name__ == "__main__":
    # Example usage
    ingestion = OpenSenseMapIngestion()
    sensor_ids = ["your-sensor-id-1", "your-sensor-id-2"]  
    ingestion.run(sensor_ids) 