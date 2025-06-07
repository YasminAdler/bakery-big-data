## To create venv in root folder 
python -m venv .venv

## Activate the venv
.venv/Scripts/activate

## Download requierments 
pip install --no-cache-dir -r orchestration/requirements.txt

## Deactivate the venv
deactivate

# Bakery Data Engineering Project

## Overview
This project implements a comprehensive data engineering solution for a bakery business, featuring real-time streaming, batch processing, data quality management, and machine learning feature engineering.

## Architecture
- **Streaming Layer**: Apache Kafka for real-time POS and IoT data
- **Storage Layer**: MinIO (S3-compatible) with Bronze/Silver/Gold architecture
- **Processing Layer**: Apache Spark for batch and stream processing
- **Table Format**: Apache Iceberg for ACID transactions and time travel
- **Orchestration**: Apache Airflow for workflow management

## Quick Start

### Prerequisites
- Docker Desktop with at least 8GB RAM allocated
- Python 3.8+
- Git

### Setup Instructions

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd bakery-data-engineering

2. **Create the shared network**
docker-compose up -d

3. **Start storage layer (MinIO)**
cd processing
docker-compose up -d minio create-buckets

4. **Start streaming layer (Kafka)**
cd ../streaming
docker-compose up -d zookeeper kafka kafka-ui

5. **Start processing layer (Spark)**
cd ../processing
docker-compose up -d spark-master spark-worker-1 spark-worker-2

6.**Start orchestration (Airflow)**
cd ../orchestration
docker-compose up -d

7.**Start data producers**
cd ../streaming
docker-compose up -d pos-producer iot-producer


**Access Points**

Airflow UI: http://localhost:8080 (admin/admin)
Kafka UI: http://localhost:8081
Spark UI: http://localhost:8082
MinIO Console: http://localhost:9001 (minioadmin/minioadmin)

## Data Pipeline
Real-time Processing

POS and IoT producers generate realistic streaming data
Kafka ingests and buffers the streams
Spark Streaming processes data with <1 minute latency
Real-time aggregations available for dashboards

Batch Processing

Daily ETL jobs orchestrated by Airflow
Bronze → Silver → Gold layer transformations
Late-arriving data handled up to 48 hours
Data quality checks at each layer

Key Features

Type 2 SCD: Tracking historical changes in pricing and customer data
Late Arrival Handling: Reconciliation process for delayed data
ML Feature Engineering: Automated feature generation for 3 use cases
Data Quality: Comprehensive validation and monitoring
Alerting: Real-time anomaly detection and maintenance alerts

Testing
Run a Complete Pipeline Test

Trigger the batch ETL DAG in Airflow
Monitor data flow through Kafka UI
Check MinIO for data in each layer
Verify data quality reports

Verify Streaming

Watch Kafka UI for incoming messages
Check Spark Streaming jobs in Spark UI
Monitor real-time aggregations

Troubleshooting
Common Issues

Services not starting: Check Docker memory allocation
Connection errors: Ensure all services are on bakery-network
Data not flowing: Check producer logs with docker logs <container>

Logs

Airflow: orchestration/logs/
Spark: Available in Spark UI
Kafka: docker logs kafka

Project Structure
bakery-data-engineering/
├── orchestration/       # Airflow DAGs and configuration
├── streaming/          # Kafka setup and data producers
├── processing/         # Spark jobs and transformations
├── docs/              # Additional documentation
└── README.md          # This file
Business Context
This solution addresses key bakery industry challenges:

Demand Forecasting: ML features for predicting product demand
Waste Reduction: Real-time monitoring and optimization
Equipment Maintenance: Predictive maintenance using IoT data
Quality Control: Automated data quality checks
