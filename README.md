# Real-Time Sensor Data Pipeline

A real-time data engineering pipeline that processes environmental sensor readings.

## Tech Stack

- Apache Kafka
- Apache Spark
- PostgreSQL
- PGadmin
- Grafana
- Docker

## Features

- Real-time streaming pipeline
- Sensor anomaly detection
- Alert generation
- Automated dashboards

## Quick Start

```bash
git clone https://github.com/Larbol624/Environmental-Sensor-Pipeline.git
cd Environmental-Sensor-Pipeline
docker network create env_network
docker compose -f docker-compose-sensor-pipeline.yml up --build -d
```

## Docs

[Architecture](docs/architecture.md)
[Set Up](docs/setup.md)
[Data flow](docs/data_flow.md)