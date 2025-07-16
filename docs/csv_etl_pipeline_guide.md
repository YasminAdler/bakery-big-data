# CSV ETL Pipeline Guide

## Overview

This guide explains how to use the new CSV ETL pipeline DAGs that process data from your CSV files through the complete Bronze → Silver → Gold data layers.

## Available DAGs

### 1. `bakery_csv_etl_pipeline` (Scheduled)
- **Schedule**: Runs every hour
- **Purpose**: Monitors for new CSV files and processes them automatically
- **Features**:
  - Infrastructure validation
  - CSV file presence checking  
  - Complete ETL pipeline (Bronze → Silver → Gold)
  - Data quality validation at each step
  - Error handling and notifications
  - Backup and cleanup operations

### 2. `bakery_csv_manual_trigger` (Manual)
- **Schedule**: Manual trigger only
- **Purpose**: Run the pipeline immediately when you have new CSV data
- **Features**:
  - Simple linear execution
  - Quick pre/post validation
  - Results summary
  - Ideal for testing and immediate processing

## How to Use

### Step 1: Prepare Your CSV Data

1. **Ensure your CSV file is in the correct format** with the expected columns:
   - Sales events data
   - Inventory updates data  
   - Equipment metrics data

2. **Place your CSV file** in the container at `/opt/spark-apps/data/bronze_combined.csv`

   ```bash
   # Copy your CSV file to the container
   docker cp your_csv_file.csv spark-submit:/opt/spark-apps/data/bronze_combined.csv
   ```

### Step 2: Choose Your Execution Method

#### Option A: Manual Trigger (Recommended for immediate processing)

1. **Access Airflow UI**: http://localhost:8081
   - Username: `airflow`
   - Password: `airflow`

2. **Find the DAG**: Look for `bakery_csv_manual_trigger`

3. **Trigger the DAG**: Click the "Trigger DAG" button (▶️)

4. **Monitor Progress**: Watch the task progress in the Graph view

#### Option B: Scheduled Processing (For regular automation)

1. **Enable the DAG**: In Airflow UI, toggle on `bakery_csv_etl_pipeline`

2. **The DAG will automatically**:
   - Check for CSV files every hour
   - Process new data when found
   - Send notifications on completion/failure

### Step 3: Monitor the Pipeline

#### Task Flow Overview
```
Infrastructure Check → CSV File Check → Bronze Loading → Silver Processing → Gold Transformation → ML Features → Results
```

#### Key Tasks to Watch:
- **`validate_infrastructure`**: Ensures all services are running
- **`check_csv_file_presence`**: Confirms CSV file exists
- **`bronze_layer_loading`**: Loads CSV into bronze tables
- **`silver_layer_processing`**: Cleanses and standardizes data
- **`gold_layer_processing`**: Creates business-ready data
- **`generate_pipeline_report`**: Shows final statistics

#### Monitoring Tools:
- **Airflow UI**: http://localhost:8081 - DAG progress and logs
- **Spark UI**: http://localhost:8080 - Job execution details
- **MinIO Console**: http://localhost:9001 - Data storage status

## Pipeline Components

### Bronze Layer (Raw Data)
- **Tables**: `sales_events`, `inventory_updates`, `equipment_metrics`
- **Purpose**: Store raw CSV data with minimal processing
- **Location**: `local.bronze.*` namespace

### Silver Layer (Standardized Data)
- **Tables**: `sales`, `inventory`, `equipment_metrics`
- **Purpose**: Cleaned, validated, and standardized data
- **Features**: Data quality scoring, late arrival handling
- **Location**: `local.silver.*` namespace

### Gold Layer (Business-Ready Data)
- **Dimensions**: Products, Stores, Customers, Calendar
- **Facts**: Sales, Inventory, Equipment Performance
- **ML Features**: Demand forecasting, maintenance prediction
- **Location**: `local.gold.*` namespace

## Data Quality Checks

The pipeline includes comprehensive data quality validation:

### Bronze Layer Validation
- File format verification
- Required column presence
- Basic data type validation

### Silver Layer Validation
- Data quality scoring (0-100)
- Business rule validation
- Duplicate detection and handling

### Gold Layer Validation
- Referential integrity checks
- Aggregation validation
- Completeness verification

## Error Handling

### Common Issues and Solutions

1. **CSV File Not Found**
   - **Error**: `check_csv_file_presence` fails
   - **Solution**: Ensure CSV file is copied to `/opt/spark-apps/data/bronze_combined.csv`

2. **Infrastructure Not Available**
   - **Error**: `validate_infrastructure` fails
   - **Solution**: Run `make health` to check service status

3. **Data Quality Issues**
   - **Error**: Quality validation tasks fail
   - **Solution**: Check data format and business rules

4. **Spark Job Failures**
   - **Error**: ETL tasks fail
   - **Solution**: Check Spark UI for detailed error messages

### Recovery Procedures

1. **Manual Retry**: Use Airflow UI to retry failed tasks
2. **Full Restart**: Trigger the manual DAG for complete reprocessing
3. **Data Rollback**: Use backup tables created during processing

## Performance Optimization

### For Large CSV Files
- Increase Spark driver/executor memory in DAG configuration
- Consider partitioning large files before processing
- Monitor MinIO storage capacity

### For Frequent Processing
- Use the scheduled DAG with appropriate interval
- Implement incremental processing for large datasets
- Monitor resource usage and adjust accordingly

## Troubleshooting

### Check Service Health
```bash
make health
```

### View Service Logs
```bash
# Airflow logs
docker logs airflow-scheduler
docker logs airflow-webserver

# Spark logs
docker logs spark-master
docker logs spark-submit

# MinIO logs
docker logs minio
```

### Verify Data
```bash
# Check data counts
make show-data

# Access Spark SQL
docker exec -it spark-submit spark-sql \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.warehouse=s3a://iceberg-warehouse/
```

## Best Practices

1. **Always validate your CSV format** before processing
2. **Use the manual trigger DAG** for testing and immediate needs
3. **Monitor data quality scores** in the Silver layer
4. **Set up proper alerts** for production environments
5. **Regular backup** of important data before processing
6. **Monitor resource usage** during large data processing

## Integration with Existing Pipeline

These DAGs integrate seamlessly with your existing bakery data pipeline:

- **Streaming Data**: Continue using Kafka producers for real-time data
- **Batch Processing**: Use these DAGs for historical data loading
- **Combined Processing**: Both can run simultaneously without conflicts

## Support

For issues or questions:
1. Check the Airflow logs in the UI
2. Review the Spark UI for job details
3. Verify service health with `make health`
4. Consult the existing pipeline documentation

The CSV ETL pipeline provides a robust, scalable solution for loading your warehouse data into the bakery analytics platform. 