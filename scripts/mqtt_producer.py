import paho.mqtt.client as mqtt
import json
import time
from typing import Dict, Any
from loguru import logger
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

class MQTTProducer:
    def __init__(self):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish
        self.client.connect(settings.MQTT_BROKER, settings.MQTT_PORT, 60)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        """Callback when connected to MQTT broker."""
        if rc == 0:
            logger.info("Connected to MQTT broker")
        else:
            logger.error(f"Failed to connect to MQTT broker with code: {rc}")

    def on_publish(self, client, userdata, mid):
        """Callback when message is published."""
        logger.info(f"Message {mid} published successfully")

    def publish_sensor_data(self, sensor_id: str, data: Dict[str, Any]):
        """Publish sensor data to MQTT topic."""
        try:
            topic = f"sensors/{sensor_id}"
            payload = json.dumps(data)
            self.client.publish(topic, payload)
            logger.info(f"Published data to topic {topic}")
        except Exception as e:
            logger.error(f"Error publishing data: {str(e)}")

    def run(self, sensor_ids: list, interval: int = 60):
        """Main producer loop."""
        while True:
            for sensor_id in sensor_ids:
                data = {
                    'sensor_id': sensor_id,
                    'value': 10.5,  
                    'timestamp': time.time(),
                    'metadata': {'location': 'room1'}
                }
                self.publish_sensor_data(sensor_id, data)
            time.sleep(interval)
    def close(self):
        """Close MQTT connection."""
        self.client.loop_stop()
        self.client.disconnect()
if __name__ == "__main__":
    producer = MQTTProducer()
    sensor_ids = ["temp_sensor_1", "humidity_sensor_1"]  
    try:
        producer.run(sensor_ids)
    except KeyboardInterrupt:
        producer.close() 