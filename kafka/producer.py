from kafka import KafkaProducer
import json
import time
from typing import Dict, Any, List
from datetime import datetime, timedelta
from loguru import logger
import os
from dotenv import load_dotenv
import requests

load_dotenv()

class OpenSenseMapKafkaProducer:
    def __init__(self):
        self.api_key = os.getenv('OPENSENSEMAP_API_KEY')
        self.base_url = "https://api.opensensemap.org"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            api_version=(2, 0, 0)
        )
        logger.info("Kafka producer initialized")

    def get_sensor_data(self, sensor_id: str, from_date: datetime, to_date: datetime) -> List[Dict[str, Any]]:
        """Fetch sensor data from OpenSenseMap."""
        try:
            endpoint = f"{self.base_url}/boxes/{sensor_id}/data"
            params = {
                "from-date": from_date.isoformat(),
                "to-date": to_date.isoformat()
            }
            
            response = requests.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched data for sensor {sensor_id}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from OpenSenseMap: {str(e)}")
            return []

    def get_sensor_info(self, sensor_id: str) -> Dict[str, Any]:
        """Fetch sensor information from OpenSenseMap."""
        try:
            endpoint = f"{self.base_url}/boxes/{sensor_id}"
            response = requests.get(endpoint, headers=self.headers)
            response.raise_for_status()
            
            info = response.json()
            logger.info(f"Successfully fetched info for sensor {sensor_id}")
            return info
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching sensor info from OpenSenseMap: {str(e)}")
            return {}

    def publish_sensor_data(self, sensor_id: str, data: Dict[str, Any], sensor_info: Dict[str, Any]):
        """Publish sensor data to Kafka topic."""
        try:
            topic = f"sensors.{sensor_id}"
            message = {
                **data,
                'sensor_info': {
                    'name': sensor_info.get('name'),
                    'model': sensor_info.get('model'),
                    'location': sensor_info.get('location'),
                    'exposure': sensor_info.get('exposure')
                }
            }
            self.producer.send(topic, value=message)
            self.producer.flush()
            logger.info(f"Published data to topic {topic}")
        except Exception as e:
            logger.error(f"Error publishing data: {str(e)}")

    def run(self, sensor_ids: List[str], interval: int = 3600):
        """Main producer loop."""
        while True:
            for sensor_id in sensor_ids:
                # Get sensor info
                sensor_info = self.get_sensor_info(sensor_id)
                if not sensor_info:
                    continue

                # Calculate time range (last hour)
                end_time = datetime.now()
                start_time = end_time - timedelta(hours=1)

                # Fetch and publish sensor data
                data_list = self.get_sensor_data(sensor_id, start_time, end_time)
                for data in data_list:
                    self.publish_sensor_data(sensor_id, data, sensor_info)

            time.sleep(interval)

    def close(self):
        """Close Kafka producer."""
        self.producer.close()
        logger.info("Kafka producer closed")

if __name__ == "__main__":
    # List of OpenSenseMap sensor IDs to fetch data from
    sensor_ids = [
        "680eaa270f126f0007ce5162",  
        "680eaa270f126f0007ce5163",
        "680eaa270f126f0007ce5164"
    ]
    
    producer = OpenSenseMapKafkaProducer()
    try:
        producer.run(sensor_ids)
    except KeyboardInterrupt:
        producer.close() 