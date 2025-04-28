from pydantic_settings import BaseSettings
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()
class Settings(BaseSettings):
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "sensor-data")
    KAFKA_CONSUMER_GROUP: str = os.getenv("KAFKA_CONSUMER_GROUP", "sensor-data-group")
    # Database Configuration
    INFLUXDB_URL: str = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    INFLUXDB_TOKEN: str = os.getenv("INFLUXDB_TOKEN", "")
    INFLUXDB_ORG: str = os.getenv("INFLUXDB_ORG", "my-org")
    INFLUXDB_BUCKET: str = os.getenv("INFLUXDB_BUCKET", "sensor-data")
    # TimescaleDB Configuration
    TIMESCALEDB_HOST: str = os.getenv("TIMESCALEDB_HOST", "localhost")
    TIMESCALEDB_PORT: int = int(os.getenv("TIMESCALEDB_PORT", "5432"))
    TIMESCALEDB_DB: str = os.getenv("TIMESCALEDB_DB", "sensor_data")
    TIMESCALEDB_USER: str = os.getenv("TIMESCALEDB_USER", "postgres")
    TIMESCALEDB_PASSWORD: str = os.getenv("TIMESCALEDB_PASSWORD", "postgres")
    class Config:
        env_file = ".env"

settings = Settings() 