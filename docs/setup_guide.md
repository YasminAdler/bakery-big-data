# Bakery Data Pipeline - Setup Guide

## Prerequisites

Before setting up the bakery data pipeline, ensure you have the following installed:

- **Docker Desktop** (version 20.10 or higher)
- **Docker Compose** (version 2.0 or higher)
- **Git** (for cloning the repository)
- **Minimum System Requirements**:
  - 8GB RAM (16GB recommended)
  - 20GB free disk space
  - 4 CPU cores

## Quick Start Guide

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd bakery-data-pipeline
```

### Step 2: Create Docker Network

First, create the shared Docker network that all services will use:

```bash
docker network create bakery-network
```

### Step 3: Start Services

Start all services in the correct order:

```bash
# 1. Start Kafka streaming services
cd streaming
docker-compose up -d
cd ..

# 2. Start Spark processing services
cd processing
docker-compose up -d
cd ..

# 3. Start Airflow orchestration
cd orchestration
docker-compose up -d
cd ..
```

### Step 4: Wait for Services to Initialize

Services need time to start up properly. Wait approximately 2-3 minutes, then check status:

```bash
# Check all running containers
docker ps

# You should see containers for:
# - kafka-broker, zookeeper, kafka-ui
# - spark-master, spark-worker, minio
# - airflow-webserver, airflow-scheduler, postgres
```

### Step 5: Initialize Infrastructure

Access Airflow UI and run the initialization DAG:

1. Open http://localhost:8080 in your browser
2. Login with username: `airflow`, password: `airflow`
3. Find the `bakery_init_infrastructure` DAG
4. Toggle it ON and trigger it manually
5. Wait for all tasks to complete (green)

### Step 6: Start Data Generation

Start the Kafka producers to generate streaming data:

```bash
cd streaming
docker-compose exec kafka-producer python /app/run_all_producers.py
```

## Service Access Points

Once everything is running, you can access:

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark UI | http://localhost:4040 | No auth required |
| Kafka UI | http://localhost:8090 | No auth required |

## Verifying the Setup

### 1. Check Kafka Topics

In Kafka UI (http://localhost:8090), verify these topics exist:
- sales-events
- equipment-metrics
- inventory-updates

### 2. Check MinIO Buckets

In MinIO Console (http://localhost:9001), verify these buckets:
- bronze
- silver
- gold
- iceberg-warehouse

### 3. Check Airflow DAGs

In Airflow UI, you should see:
- `bakery_init_infrastructure` (should be successful)
- `bakery_batch_etl` (ready to run)
- `bakery_streaming_manager` (ready to run)

### 4. Verify Iceberg Tables

Run this command to list all tables:

```bash
docker exec spark-submit spark-sql \
  --master spark://spark-master:7077 \
  -e "SHOW DATABASES IN local;" \
  -e "SHOW TABLES IN local.bronze;" \
  -e "SHOW TABLES IN local.silver;" \
  -e "SHOW TABLES IN local.gold;"
```

## Running the Pipeline

### Streaming Pipeline

The streaming pipeline runs continuously:

1. Enable the `bakery_streaming_manager` DAG in Airflow
2. It will monitor and manage the streaming jobs
3. Check Spark UI for active streaming queries

### Batch Pipeline

The batch pipeline runs daily at 2 AM:

1. Enable the `bakery_batch_etl` DAG in Airflow
2. You can also trigger it manually for testing
3. Monitor progress in Airflow UI

## Monitoring

### Real-time Monitoring

- **Kafka UI**: Monitor message flow and consumer lag
- **Spark UI**: Track streaming query progress
- **Airflow UI**: View DAG runs and task logs

### Data Quality

Check data quality scores in the Silver layer:

```bash
docker exec spark-submit spark-sql \
  --master spark://spark-master:7077 \
  -e "SELECT AVG(data_quality_score) FROM local.silver.sales;"
```

## Troubleshooting

### Common Issues

#### 1. Services Not Starting

```bash
# Check container logs
docker logs <container-name>

# Restart a specific service
cd <component-directory>
docker-compose restart <service-name>
```

#### 2. Kafka Connection Issues

```bash
# Test Kafka connectivity
docker exec kafka-broker kafka-broker-api-versions \
  --bootstrap-server localhost:9092
```

#### 3. Spark Job Failures

```bash
# Check Spark logs
docker logs spark-master
docker logs spark-worker
```

#### 4. MinIO Access Issues

```bash
# Test MinIO connectivity
docker exec minio mc admin info myminio
```

### Resetting the Environment

If you need to start fresh:

```bash
# Stop all services
cd streaming && docker-compose down -v && cd ..
cd processing && docker-compose down -v && cd ..
cd orchestration && docker-compose down -v && cd ..

# Remove the network
docker network rm bakery-network

# Start again from Step 2
```

## Data Flow Architecture

```
Kafka Producers → Kafka Topics → Spark Streaming → Bronze Layer (MinIO/Iceberg)
                                          ↓
                                  Airflow Batch ETL
                                          ↓
                              Silver Layer (Cleansed Data)
                                          ↓
                              Gold Layer (Business Ready)
```

## Performance Tuning

### Spark Configuration

Edit `processing/config/spark-defaults.conf` to adjust:
- Memory allocation
- Executor cores
- Parallelism settings

### Kafka Configuration

Edit `streaming/docker-compose.yml` to adjust:
- Partition counts
- Replication factors
- Buffer sizes

### Airflow Configuration

Edit `orchestration/docker-compose.yml` to adjust:
- Worker count
- Task concurrency
- DAG refresh interval

## Security Considerations

**Note**: This setup uses default credentials for demo purposes. For production:

1. Change all default passwords
2. Enable SSL/TLS for all services
3. Implement proper authentication
4. Use secrets management
5. Enable audit logging

## Next Steps

1. Explore the data in MinIO using Spark SQL
2. Create custom dashboards using the Gold layer
3. Implement additional data quality checks
4. Add new data sources
5. Scale out Spark workers for better performance 