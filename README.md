## To create venv in root folder 
python -m venv .venv

## Activate the venv
.venv/Scripts/activate

## Download requierments 
pip install --no-cache-dir -r orchestration/requirements.txt

## Deactivate the venv
deactivate

# Bakery Data Engineering Project

## To start the program: 

run your docker desktop 

### To start everything:
make start

### To initialize the topics and tables after starting:
make init

### To shut everything down completely:
make down




## Overview
This project implements a comprehensive data engineering solution for a bakery business, featuring real-time streaming, batch processing, data quality management, and machine learning feature engineering.

## Architecture
- **Streaming Layer**: Apache Kafka for real-time POS and IoT data
- **Storage Layer**: MinIO (S3-compatible) with Bronze/Silver/Gold architecture
- **Processing Layer**: Apache Spark for batch and stream processing
- **Table Format**: Apache Iceberg for ACID transactions and time travel
- **Orchestration**: Apache Airflow for workflow management

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

Airflow UI: http://localhost:8080

username: admin 
password: admin

Kafka UI: http://localhost:8081

Spark UI: http://localhost:8082

MinIO Console: http://localhost:9001 

username: minioadmin
password: minioadmin

