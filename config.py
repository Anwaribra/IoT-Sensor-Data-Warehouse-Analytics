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

    # MQTT Configuration
    MQTT_BROKER: str = os.getenv("MQTT_BROKER", "localhost")
    MQTT_PORT: int = int(os.getenv("MQTT_PORT", "1883"))

    # OpenSenseMap API
    OPENSENSEMAP_API_URL: str = os.getenv("OPENSENSEMAP_API_URL", "https://api.opensensemap.org")
    OPENSENSEMAP_API_KEY: Optional[str] = os.getenv("OPENSENSEMAP_API_KEY")

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

    # Monitoring Configuration
    PROMETHEUS_PORT: int = int(os.getenv("PROMETHEUS_PORT", "9090"))
    GRAFANA_URL: str = os.getenv("GRAFANA_URL", "http://localhost:3000")
    GRAFANA_API_KEY: Optional[str] = os.getenv("GRAFANA_API_KEY")

    # Security Configuration
    JWT_SECRET_KEY: str = os.getenv("JWT_SECRET_KEY", "your-secret-key")
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    # Data Quality Configuration
    DATA_QUALITY_THRESHOLD: float = float(os.getenv("DATA_QUALITY_THRESHOLD", "0.95"))
    ANOMALY_DETECTION_THRESHOLD: float = float(os.getenv("ANOMALY_DETECTION_THRESHOLD", "3.0"))

    class Config:
        env_file = ".env"

settings = Settings() 