# Bakery Data Pipeline - End-to-End Data Engineering Solution

## 🥐 Project Overview

This project implements a comprehensive data engineering solution for a bakery chain, featuring real-time streaming, batch processing, and advanced analytics capabilities. The pipeline processes sales events, inventory updates, customer feedback, and equipment metrics through Bronze, Silver, and Gold data layers.

### 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Data Sources  │     │   Processing    │     │   Data Layers   │
├─────────────────┤     ├─────────────────┤     ├─────────────────┤
│ • Kafka Streams │────▶│ • Apache Spark  │────▶│ • Bronze (Raw)  │
│ • Batch Files   │     │ • Stream & Batch│     │ • Silver (Clean)│
│ • APIs          │     │ • Quality Checks│     │ • Gold (Business)│
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                       │                        │
         └───────────────────────┴────────────────────────┘
                               │
                        ┌──────▼──────┐
                        │   Airflow   │
                        │ Orchestrator│
                        └─────────────┘
```

##  Quick Start
### One-Command Setup (Recommended)

Run **all** services and initialize the complete environment with a single command:

```bash
make start
```

This will automatically:
1. Build every Docker image
2. Create the shared `bakery-network`
3. Launch Kafka, Spark, Airflow, and MinIO containers
4. Wait until each service is healthy
5. Initialise MinIO buckets, Kafka topics, and all Iceberg tables
6. Print the status and access URLs

Once finished open the UIs:

| Service | URL | Credentials |
|---------|-----|-------------|
| Spark UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Airflow UI | http://localhost:8081 | airflow / airflow |
| Kafka UI | http://localhost:8090 | — |

Need to stop everything? Use `make stop` (containers remain) or `make clean` (full cleanup).

### Manual Setup (Alternative)

### Prerequisites
- Docker Desktop installed and running
- At least 8GB RAM available
- 20GB free disk space

### 1️⃣ Clone the Repository
```bash
git clone <repository-url>
cd bakery-data-pipeline
```

### 2️⃣ Start All Services
```bash
# Start all services with a single command
docker-compose -f orchestration/docker-compose.yml -f streaming/docker-compose.yml -f processing/docker-compose.yml up -d
```

### 3️⃣ Run Initial Setup
```bash
# Create MinIO buckets
docker exec -it minio mc alias set myminio http://minio:9000 minioadmin minioadmin
docker exec -it minio mc mb myminio/bronze
docker exec -it minio mc mb myminio/silver
docker exec -it minio mc mb myminio/gold

# Start data generators
docker exec -it kafka-producer python /app/generate_sales_events.py
```

## 📊 Data Flow

### Bronze Layer (Raw Data)
- `bronze_sales_events` - Real-time sales transactions
- `bronze_inventory_updates` - Inventory changes (handles late arrivals)
- `bronze_customer_feedback` - Customer reviews and ratings
- `bronze_promotions` - Marketing campaigns
- `bronze_weather_data` - Weather conditions
- `bronze_equipment_metrics` - Equipment IoT sensor data

### Silver Layer (Standardized)
- Cleaned and validated data
- Standardized formats
- Data quality scores
- Partitioned by date

### Gold Layer (Business-Ready)
- **Dimensions**: Store, Product, Customer, Equipment, Calendar, Weather
- **Facts**: Sales, Inventory, Promotions, Customer Feedback, Equipment Performance
- **ML Features**: Demand Forecasting, Maintenance Prediction, Quality Prediction

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Storage | MinIO + Apache Iceberg | S3-compatible object storage with ACID transactions |
| Streaming | Apache Kafka | Real-time event streaming |
| Processing | Apache Spark | Batch and stream processing |
| Orchestration | Apache Airflow | Workflow scheduling and monitoring |
| Containerization | Docker Compose | Service isolation and deployment |

## 📁 Project Structure
```
bakery-data-pipeline/
├── orchestration/          # Airflow DAGs and configuration
│   ├── dags/              # DAG definitions
│   ├── plugins/           # Custom operators
│   └── docker-compose.yml # Airflow services
├── streaming/             # Kafka setup and producers
│   ├── producers/         # Python data generators
│   ├── config/           # Kafka configuration
│   └── docker-compose.yml # Kafka services
├── processing/            # Spark applications
│   ├── jobs/             # ETL job definitions
│   ├── config/           # Spark configuration
│   └── docker-compose.yml # Spark services
├── docs/                  # Additional documentation
│   ├── data_models.md    # Mermaid diagrams
│   └── setup_guide.md    # Detailed setup instructions
└── README.md             # This file
```

## 🔄 Key Features

### Real-Time Processing
- Kafka streams for sales events and equipment metrics
- Spark Structured Streaming for low-latency processing
- Exactly-once semantics with Iceberg

### Late Arrival Handling
- 48-hour window for late-arriving inventory data
- Event time processing with watermarks
- Automatic reprocessing of updated records

### Data Quality
- Validation checks at each layer
- Data quality scoring
- Automated alerting for anomalies

### SCD Type 2 Implementation
- Historical tracking for Store and Product Pricing dimensions
- Effective date management
- Current record flagging

## 📈 Analytics Capabilities

### Business Intelligence
- Sales performance dashboards
- Inventory optimization metrics
- Customer satisfaction analysis
- Equipment efficiency monitoring

### Machine Learning Features
- Demand forecasting with seasonality
- Predictive maintenance for equipment
- Product quality prediction
- Customer churn analysis

## 🛠️ Makefile Cheat-Sheet

| Command | What it does |
|---------|--------------|
| `make start` | Build images, start all services, initialize infrastructure |
| `make init` | (Re)create buckets, topics, and Iceberg tables |
| `make batch-etl` | Run Bronze→Silver→Gold batch ETL jobs |
| `make show-data` | Quick record counts in Bronze/Silver/Gold |
| `make stop` | Stop all running containers |
| `make clean` | Remove containers, volumes, and network |

Run any target with `make <target>`—for example `make producers` to begin generating real-time data.

## 🐛 Troubleshooting

### Common Issues

1. **Services not starting**: Check Docker memory allocation (minimum 4GB)
2. **Kafka connection errors**: Ensure all services are in the same Docker network
3. **Spark job failures**: Check MinIO bucket permissions
4. **Airflow DAG errors**: Review logs in Airflow UI

### Logs Location
- Airflow: `./orchestration/logs/`
- Spark: Access via Spark UI or container logs
- Kafka: `docker logs kafka-broker`

