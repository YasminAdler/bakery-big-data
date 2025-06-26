# Iceberg Table Creation Flow Fix

## Problem Identified

Your existing Airflow DAGs were attempting to read from and write to Iceberg tables that didn't exist yet. This is a common initialization issue where:

1. ETL jobs assumed tables like `local.db.bronze_sales_events`, `local.db.silver_sales`, etc. already existed
2. No initialization step was creating these tables before the ETL processes ran
3. This would cause failures when trying to run the batch ETL DAG

## Solution Implemented

### 1. Created Table Initialization Script

**File:** `processing/spark_jobs/ETL/create_iceberg_tables.py`

This script creates all required Iceberg tables with proper schemas for:
- **Bronze Layer:** Raw data tables (sales events, inventory updates, customer feedback, equipment metrics)
- **Silver Layer:** Cleaned and enriched data tables
- **Gold Layer:** Aggregated analytics tables

Key features:
- Configurable to create specific layers (`bronze`, `silver`, `gold`, or `all`)
- Proper Iceberg table properties for performance
- Date-based partitioning for efficient querying
- Comprehensive error handling
- Table verification functionality

### 2. Created Initialization DAG

**File:** `orchestration/dags/create_iceberg_tables_dag.py`

This DAG orchestrates the table creation process:
- Manual trigger only (no automatic scheduling)
- Creates tables layer by layer (bronze → silver → gold)
- Includes verification step
- Provides clear success/failure notifications

### 3. Updated Batch ETL DAG

**File:** `orchestration/dags/batch_etl_dag.py`

Enhanced the existing DAG with:
- Table existence check before ETL processes start
- Clear error messages if tables are missing
- Proper dependency chain to ensure table availability

## How to Use This Solution

### Initial Setup (One-time)

1. **First Time Setup:** Run the table creation DAG
   ```bash
   # In Airflow UI, manually trigger the DAG:
   # "create_iceberg_tables"
   ```

2. **Alternative: Create specific layers**
   You can also run table creation for specific layers if needed:
   ```bash
   # Create only bronze tables
   spark-submit create_iceberg_tables.py bronze
   
   # Create only silver tables  
   spark-submit create_iceberg_tables.py silver
   
   # Create only gold tables
   spark-submit create_iceberg_tables.py gold
   
   # Create all tables
   spark-submit create_iceberg_tables.py all
   ```

### Regular Operations

1. **Run Table Creation DAG** (if not already done)
   - Go to Airflow UI
   - Find "create_iceberg_tables" DAG
   - Click "Trigger DAG"
   - Wait for completion ✅

2. **Run Regular ETL DAGs**
   - The "bakery_batch_etl" DAG now automatically checks for table existence
   - If tables don't exist, it will fail with a clear error message
   - If tables exist, ETL proceeds normally

## Table Schema Overview

### Bronze Layer Tables
- `bronze_sales_events`: Raw POS transaction data
- `bronze_inventory_updates`: Raw inventory movement data
- `bronze_customer_feedback`: Raw customer feedback data
- `bronze_equipment_metrics`: Raw IoT sensor data

### Silver Layer Tables
- `silver_sales`: Cleaned and enriched sales data
- `silver_inventory`: Processed inventory data with calculations
- `silver_customer_feedback`: Processed feedback with sentiment analysis
- `silver_equipment_metrics`: Processed equipment data with alerts

### Gold Layer Tables
- `gold_daily_sales_summary`: Daily aggregated sales metrics
- `gold_customer_analytics`: Customer behavior analytics
- `gold_inventory_optimization`: Inventory optimization recommendations
- `gold_ml_features`: Feature vectors for machine learning

## Troubleshooting

### Common Issues and Solutions

1. **"Table does not exist" errors**
   ```
   Solution: Run the create_iceberg_tables DAG first
   ```

2. **S3/MinIO connection issues**
   ```
   Check: 
   - MinIO is running
   - Credentials are correct
   - Network connectivity
   ```

3. **Spark configuration issues**
   ```
   Verify:
   - Iceberg jars are available
   - Spark catalog configuration is correct
   - Warehouse path is accessible
   ```

4. **Permission errors**
   ```
   Check:
   - S3 bucket permissions
   - Spark service account permissions
   ```

### Verification Commands

Check if tables exist:
```sql
-- In Spark SQL shell
SHOW TABLES IN local.db;

-- Check specific table
DESCRIBE local.db.bronze_sales_events;
```

### Re-creating Tables

If you need to recreate tables:
```sql
-- Drop and recreate (BE CAREFUL - THIS DELETES DATA!)
DROP TABLE IF EXISTS local.db.bronze_sales_events;
```

Then run the table creation DAG again.

## Best Practices

1. **Always run table creation before ETL**
   - Especially in new environments
   - After any schema changes

2. **Monitor table creation logs**
   - Check for any creation failures
   - Verify all tables are accessible

3. **Use the verification step**
   - Don't skip the table verification
   - Address any reported issues immediately

4. **Environment-specific considerations**
   - Development: Recreate tables as needed for testing
   - Production: Be careful with schema changes

## Architecture Benefits

This solution provides:
- ✅ **Reliability**: ETL won't fail due to missing tables
- ✅ **Maintainability**: Clear separation of concerns
- ✅ **Scalability**: Easy to add new tables
- ✅ **Observability**: Clear error messages and verification
- ✅ **Consistency**: Standardized table creation process

## Next Steps

After fixing the table creation flow, you should:
1. Test the complete pipeline end-to-end
2. Set up monitoring for table health
3. Document any custom schema requirements
4. Plan for schema evolution procedures