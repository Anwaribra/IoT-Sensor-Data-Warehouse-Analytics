import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import json
from loguru import logger
import os
from dotenv import load_dotenv

load_dotenv()

class MQTTKafkaBridge:
    def __init__(self):
        # MQTT client
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message
        
        # Kafka producer with Docker host
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=['host.docker.internal:9092'],  
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

    def on_mqtt_connect(self, client, userdata, flags, rc):
        """Callback when connected to MQTT broker."""
        if rc == 0:
            logger.info("Connected to MQTT broker")
            self.mqtt_client.subscribe("sensors/#")
        else:
            logger.error(f"Failed to connect to MQTT broker with code: {rc}")

    def on_mqtt_message(self, client, userdata, msg):
        """Callback when message is received from MQTT."""
        try:
            # MQTT message
            payload = json.loads(msg.payload.decode())
            
            # Forward to Kafka
            self.kafka_producer.send('sensor-data', value=payload)
            self.kafka_producer.flush()
            
            logger.info(f"Forwarded message from MQTT topic {msg.topic} to Kafka")
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")

    def run(self):
        """Start the bridge."""
        try:
            # Connect to MQTT broker
            self.mqtt_client.connect('localhost', 1883, 60)
            self.mqtt_client.loop_forever()
        except KeyboardInterrupt:
            logger.info("Shutting down bridge...")
        finally:
            self.close()

    def close(self):
        """Close all connections."""
        self.mqtt_client.disconnect()
        self.kafka_producer.close()

if __name__ == "__main__":
    bridge = MQTTKafkaBridge()
    bridge.run() 