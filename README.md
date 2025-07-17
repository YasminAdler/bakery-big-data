# Bakery Data Pipeline - End-to-End Data Engineering Solution

## �� Project Overview

A comprehensive data engineering solution for a bakery chain implementing the **Bronze-Silver-Gold** medallion architecture. Processes real-time sales events, inventory updates, customer feedback, and equipment metrics using Apache Spark, Apache Iceberg, Apache Airflow, and Kafka.

###  Architecture

```
Data Sources → Kafka → Spark Processing → Iceberg Tables (Bronze/Silver/Gold) → Analytics
     ↓              ↓           ↓                    ↓
  APIs/Files    Streaming    ETL Jobs         Business Reports
                              ↓
                        Airflow Orchestration
```

##  Quick Start

### Prerequisites
- **WSL (Windows Subsystem for Linux)** - Required for proper file system handling
- Docker and Docker Compose
- Make

### One-Command Setup

```bash
wsl
cd /path/to/bakery-big-data

make start
```

This command will:
1. Build all Docker images
2. Start Kafka, Spark, Airflow, and MinIO services
3. Initialize buckets, topics, and Iceberg tables
4. Set up the complete data pipeline

### Access URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8081 | airflow / airflow |
| Spark UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8090 | — |

## 📊 Data Layers

### Bronze Layer (Raw Data)
- `bronze_sales_events` - Real-time sales transactions
- `bronze_inventory_updates` - Inventory changes
- `bronze_customer_feedback` - Customer reviews
- `bronze_equipment_metrics` - Equipment IoT data

### Silver Layer (Cleaned & Standardized)
- Validated and cleaned data
- Data quality scoring
- Partitioned by date

### Gold Layer (Business-Ready)
- **Dimensions**: Store, Product, Customer, Calendar
- **Facts**: Sales, Inventory, Equipment Performance
- **SCD Type 2**: Historical tracking for stores and product pricing

## 📁 Project Structure

```
bakery-big-data/
├── orchestration/          # Airflow DAGs and services
├── streaming/             # Kafka producers and configuration
├── processing/            # Spark ETL jobs
├── docs/                  # Documentation
└── miri2_dashboard/       # Dashboard application
```

## 🔄 Key Features

- **Real-Time Processing**: Kafka streams with Spark Structured Streaming
- **Batch Processing**: Automated ETL pipelines (Bronze → Silver → Gold)
- **Data Quality**: Validation and quality scoring at each layer
- **Late Arrival Handling**: 48-hour window for late data
- **Orchestration**: Airflow DAGs for automated pipeline execution

## 🛠️ Common Commands

| Command | Description |
|---------|-------------|
| `make start` | Start all services and initialize infrastructure |
| `make init` | Reinitialize buckets, topics, and tables |
| `make batch-etl` | Run Bronze→Silver→Gold ETL pipeline |
| `make show-data` | Display record counts across all layers |
| `make stop` | Stop all services |
| `make clean` | Complete cleanup (containers, volumes, network) |

## 📝 Important Notes

- **Always use WSL** for running commands to avoid Windows path issues
- The pipeline automatically detects and processes the most recent available data
- ETL jobs include data quality checks and error handling
- Airflow DAGs run every 2 hours for automated processing

