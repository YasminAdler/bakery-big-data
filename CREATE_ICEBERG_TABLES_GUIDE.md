# Guide: Creating Iceberg Tables using Airflow DAG

This guide explains how to create all Iceberg tables (Silver, Gold, and ML Feature tables) using the Airflow DAG and Spark job.

## Prerequisites

1. **Ensure all services are running:**
   ```bash
   # Start all services
   make start
   
   # Wait for services to be ready (about 30-60 seconds)
   ```

2. **Verify services are running:**
   ```bash
   # Check service status
   make status
   ```

## Step 1: Access Airflow UI

1. Open your browser and go to: http://localhost:8080
2. Login with:
   - Username: `admin`
   - Password: `admin`

## Step 2: Enable and Trigger the DAG

1. In the Airflow UI, look for the DAG named `create_iceberg_tables_dag`
2. Toggle the DAG to "ON" (enabled) using the switch on the left
3. Click on the DAG name to open its details
4. Click the "Trigger DAG" button (play icon) in the top right
5. Click "Trigger" in the confirmation dialog

## Step 3: Monitor Execution

1. In the DAG details view, you'll see the tasks:
   - `check_minio_connection` - Verifies MinIO is accessible
   - `check_spark_connection` - Verifies Spark is ready
   - `create_all_iceberg_tables` - Creates all tables
   - `show_table_creation_summary` - Shows summary of created tables

2. Click on a task to see its logs and status
3. The entire process should take 2-5 minutes

## Step 4: Verify Tables Were Created

You can verify the tables in several ways:

### Option 1: Check MinIO Console
1. Open http://localhost:9001
2. Login with:
   - Username: `minioadmin`
   - Password: `minioadmin`
3. Browse the buckets:
   - `silver/` - Contains Silver layer tables
   - `gold/` - Contains Gold layer tables
   - `warehouse/` - Contains Iceberg metadata

### Option 2: Run Spark Shell
```bash
# Connect to Spark container
docker exec -it spark-master /bin/bash

# Start Spark shell with Iceberg
spark-shell \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true

# In Spark shell, list tables
spark.sql("SHOW TABLES").show()
```

## Tables Created

### Silver Layer Tables
- `silver_sales` - POS transactions with data quality scores
- `silver_inventory` - Inventory updates with waste ratios
- `silver_customer_feedback` - Customer reviews with sentiment
- `silver_equipment_metrics` - Equipment performance data

### Gold Layer - Dimension Tables
- `dim_equipment` - Equipment master data
- `dim_store` - Store information (SCD Type 2)
- `dim_product` - Product catalog
- `dim_product_pricing` - Product pricing (SCD Type 2)
- `dim_customer` - Customer information
- `dim_calendar` - Date dimension
- `dim_weather` - Weather conditions
- `dim_marketing_events` - Marketing campaigns

### Gold Layer - Fact Tables
- `fact_sales` - Sales transactions
- `fact_inventory` - Inventory movements
- `fact_promotions` - Active promotions
- `fact_customer_feedback` - Customer feedback
- `fact_equipment_performance` - Equipment metrics

### ML Feature Tables
- `fact_demand_forecast_features` - Demand forecasting features
- `fact_equipment_maintenance_features` - Predictive maintenance features
- `fact_product_quality_features` - Quality prediction features

## Troubleshooting

### If the DAG fails:

1. **Check MinIO is running:**
   ```bash
   docker ps | grep minio
   ```

2. **Check Spark is running:**
   ```bash
   docker ps | grep spark
   ```

3. **View task logs in Airflow UI:**
   - Click on the failed task
   - Click "Log" to see detailed error messages

4. **Common issues:**
   - MinIO not started: Run `make start`
   - Spark not ready: Wait a few minutes and retry
   - Network issues: Ensure `bakery-network` exists

### Manual Table Creation (Alternative)

If the DAG doesn't work, you can create tables manually:

```bash
# Run the create_iceberg_tables.py script directly
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  /opt/spark/jobs/ETL/create_iceberg_tables.py
```

## Next Steps

After creating the tables:

1. **Start data producers:**
   ```bash
   make start-producers
   ```

2. **Run ETL pipelines:**
   - Enable and trigger `bronze_to_silver_etl_dag`
   - Enable and trigger `silver_to_gold_etl_dag`

3. **Monitor data flow:**
   - Check Kafka UI: http://localhost:8081
   - Check Spark UI: http://localhost:8082
   - Check MinIO: http://localhost:9001

## Important Notes

- Tables are created only once. Re-running won't recreate existing tables
- The tables use Apache Iceberg format for ACID transactions and time travel
- All tables are partitioned appropriately for optimal performance
- The process creates the complete data lake structure for the bakery project