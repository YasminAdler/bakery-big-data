# MinIO Buckets Setup Guide

This guide explains how to create and manage the bronze, silver, and gold buckets in MinIO for your bakery data lake.

## Overview

Your Iceberg tables are configured to use the following S3-compatible paths:
- `s3a://bronze/` - For raw data ingestion
- `s3a://silver/` - For cleaned and transformed data
- `s3a://gold/` - For business-ready data (dimensions and facts)
- `s3a://warehouse/` - For the Iceberg warehouse/metadata

## Automatic Bucket Creation

### Method 1: Using Docker Compose (Recommended)

The buckets are automatically created when you start the services. The `create-buckets` service in `processing/docker-compose.yml` handles this:

```bash
# Start all services (includes bucket creation)
make start

# Or specifically run the initialization
make init
```

### Method 2: Check if Buckets Exist

To verify that buckets have been created:

```bash
# Using the makefile command
make check-buckets

# Or access MinIO UI
# Navigate to http://localhost:9001
# Login with: minioadmin / minioadmin
```

## Manual Bucket Creation

### Method 1: Using MinIO Client (mc) via Docker

```bash
# Run MinIO client commands
docker compose -f processing/docker-compose.yml exec minio sh -c "
mc alias set myminio http://localhost:9000 minioadmin minioadmin
mc mb myminio/bronze
mc mb myminio/silver
mc mb myminio/gold
mc mb myminio/warehouse
mc ls myminio/
"
```

### Method 2: Using the Python Script

```bash
# From within a Spark container or any container with Python and boto3
docker compose -f processing/docker-compose.yml exec spark-master python /opt/spark/jobs/utils/create_minio_buckets.py

# Or run it locally (change endpoint to localhost:9000 in the script)
cd processing/utils
python create_minio_buckets.py
```

### Method 3: Using MinIO Console UI

1. Navigate to http://localhost:9001
2. Login with credentials: `minioadmin` / `minioadmin`
3. Click on "Buckets" in the left sidebar
4. Click "Create Bucket"
5. Create buckets named: `bronze`, `silver`, `gold`, and `warehouse`

## Troubleshooting

### Buckets Not Created Automatically

If buckets aren't created automatically:

1. Check if MinIO is running:
   ```bash
   docker ps | grep minio
   ```

2. Check the create-buckets service logs:
   ```bash
   docker compose -f processing/docker-compose.yml logs create-buckets
   ```

3. Manually trigger bucket creation:
   ```bash
   docker compose -f processing/docker-compose.yml up create-buckets
   ```

### "No such bucket" Errors in Spark Jobs

If you get bucket not found errors when running Spark jobs:

1. Verify buckets exist:
   ```bash
   make check-buckets
   ```

2. Ensure the bucket names match exactly (they are case-sensitive)

3. Check MinIO connectivity from Spark:
   ```bash
   docker compose -f processing/docker-compose.yml exec spark-master sh -c "
   curl -I http://minio:9000
   "
   ```

### Permission Errors

If you encounter permission errors:

1. Verify MinIO credentials in your Spark configuration match:
   - Access Key: `minioadmin`
   - Secret Key: `minioadmin`

2. Check that the S3A filesystem is properly configured in your Spark jobs

## Best Practices

1. **Always verify buckets exist before running ETL jobs**
   ```bash
   make check-buckets
   ```

2. **Use the initialization command after starting services**
   ```bash
   make init
   ```

3. **Monitor bucket usage via MinIO Console**
   - http://localhost:9001
   - Check bucket sizes and object counts

4. **Organize data with prefixes**
   - Bronze: `/raw/source_system/year/month/day/`
   - Silver: `/cleaned/domain/table_name/`
   - Gold: `/analytics/subject_area/table_name/`

## Integration with Iceberg Tables

Your Iceberg tables are configured to use these buckets. For example:

```sql
CREATE TABLE IF NOT EXISTS spark_catalog.db.silver_sales (
    ...
)
USING iceberg
LOCATION 's3a://silver/silver_sales'
```

This creates the table data in the `silver` bucket under the `silver_sales` prefix.

## Next Steps

After creating buckets:

1. Run the Iceberg table creation DAG in Airflow
2. Start your data producers
3. Monitor data flow through bronze → silver → gold layers

For more information, see:
- [Architecture Documentation](./architecture.md)
- [Data Models Documentation](./data_models.md)