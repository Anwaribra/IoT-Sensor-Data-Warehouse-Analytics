# IoT Data Warehouse

A simple data pipeline for IoT sensor data collection and analysis.

## Overview

This project processes IoT telemetry data from CSV files, stores it in a time-series database, and provides analysis capabilities.

## Components

1. **Data Source**: IoT telemetry data in CSV format
2. **Kafka**: Message broker for data streaming
3. **TimescaleDB**: Time-series database for storage
4. **Airflow**: Workflow orchestration



## Data Fields

- `timestamp`: Time of reading
- `device`: Device ID
- `co`: Carbon monoxide (ppm)
- `humidity`: Humidity (%)
- `light`: Light sensor (on/off)
- `lpg`: LPG level (ppm)
- `motion`: Motion detected (yes/no)
- `smoke`: Smoke level (ppm)
- `temp`: Temperature (°C)
