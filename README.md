# IoT Data Warehouse

A comprehensive data engineering solution for IoT sensor data collection, processing, and analysis.

## Data Flow

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  IoT Sensors    │────▶│    MQTT Broker  │────▶│  Kafka Broker  │
│                 │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Data Analysis  │◀────│  Data Pipeline  │◀────│  Kafka Consumer │
│  (Jupyter)      │     │  (Spark/Prefect)│     │                 │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │                 │
                        │   TimescaleDB   │
                        │                 │
                        └─────────────────┘
```

## Components

1. **Data Ingestion**:
   - IoT sensors send data to MQTT broker
   - MQTT to Kafka bridge forwards data to Kafka
   - Kafka consumer processes and stores data in TimescaleDB

2. **Data Processing**:
   - Apache Spark for big data processing
   - Prefect for workflow orchestration
   - Great Expectations for data quality

3. **Data Storage**:
   - TimescaleDB for time-series data storage

4. **Data Analysis**:
   - Jupyter notebooks for interactive analysis
   - Pandas, NumPy, and scikit-learn for data manipulation
   - Matplotlib, Seaborn, and Plotly for visualization

5. **Workflow Orchestration**:
   - Apache Airflow for scheduling and monitoring

6. **Monitoring**:
   - Prometheus for metrics collection
   - Grafana for visualization

## Project Structure

```
iot-data-warehouse/
├── data/                  # Data storage
├── notebooks/             # Jupyter notebooks for analysis
├── scripts/               # Python scripts
│   ├── airflow/           # Airflow DAGs
│   │   └── dags/          # DAG definitions
│   ├── data_pipeline.py   # Data processing pipeline
│   ├── mqtt_producer.py   # MQTT data producer
│   ├── mqtt_kafka_bridge.py # MQTT to Kafka bridge
│   ├── kafka_consumer.py  # Kafka consumer
│   └── data_generator.py  # Test data generator
├── docker-compose.yml     # Docker services configuration
├── requirements.txt       # Python dependencies
└── README.md              # Project documentation
```

## Getting Started

1. Start the services:
```bash
docker-compose up -d
```

2. Access the tools:
   - Jupyter Notebook: http://localhost:8888
   - Airflow UI: http://localhost:8080
   - Grafana: http://localhost:3000
   - Spark UI: http://localhost:8080

3. Run the data pipeline:
```bash
python scripts/data_pipeline.py
```