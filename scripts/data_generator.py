import random
import time
from typing import Dict, Any, List
import json
from datetime import datetime
from loguru import logger

class SensorDataGenerator:
    def __init__(self, sensor_configs: List[Dict[str, Any]]):
        self.sensor_configs = sensor_configs
        self.data_buffer = []

    def generate_sensor_data(self, sensor_config: Dict[str, Any]) -> Dict[str, Any]:
        """Generate simulated data for a sensor."""
        try:
            sensor_id = sensor_config['sensor_id']
            min_value = sensor_config.get('min_value', 0)
            max_value = sensor_config.get('max_value', 100)
            noise_level = sensor_config.get('noise_level', 0.1)
          
            base_value = random.uniform(min_value, max_value)
            
            noise = random.gauss(0, noise_level)
            value = base_value + noise
           
            data = {
                'sensor_id': sensor_id,
                'value': round(value, 2),
                'timestamp': datetime.now().isoformat(),
                'metadata': {
                    'location': sensor_config.get('location', 'unknown'),
                    'sensor_type': sensor_config.get('sensor_type', 'unknown'),
                    'unit': sensor_config.get('unit', 'unknown')
                }
            }
            return data
        except Exception as e:
            logger.error(f"Error generating sensor data: {str(e)}")
            return {}

    def generate_batch(self, batch_size: int = 100) -> List[Dict[str, Any]]:
        """Generate a batch of sensor data."""
        try:
            batch = []
            for _ in range(batch_size):
                for sensor_config in self.sensor_configs:
                    data = self.generate_sensor_data(sensor_config)
                    if data:
                        batch.append(data)
            return batch
        except Exception as e:
            logger.error(f"Error generating batch: {str(e)}")
            return []

    def save_to_file(self, data: List[Dict[str, Any]], filename: str):
        """Save generated data to a file."""
        try:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Saved data to {filename}")
        except Exception as e:
            logger.error(f"Error saving data to file: {str(e)}")

    def run(self, output_file: str, interval: int = 60, batch_size: int = 100):
        """Main generator loop."""
        while True:
            batch = self.generate_batch(batch_size)
            if batch:
                self.save_to_file(batch, output_file)
            time.sleep(interval)

if __name__ == "__main__":
    # sensor configurations
    sensor_configs = [
        {
            'sensor_id': 'temp_sensor_1',
            'min_value': 15,
            'max_value': 30,
            'noise_level': 0.5,
            'location': 'room1',
            'sensor_type': 'temperature',
            'unit': 'celsius'
        },
        {
            'sensor_id': 'humidity_sensor_1',
            'min_value': 30,
            'max_value': 70,
            'noise_level': 1.0,
            'location': 'room1',
            'sensor_type': 'humidity',
            'unit': 'percent'
        }
    ]
    
  
    generator = SensorDataGenerator(sensor_configs)
    output_file = "data/raw_data/sensor_data.json"
    try:
        generator.run(output_file)
    except KeyboardInterrupt:
        logger.info("Stopping data generator") 