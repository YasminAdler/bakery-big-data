# Fix: Airflow DAGs Not Showing in UI

If you don't see any DAGs in your Airflow UI, follow these steps:

## Step 1: Restart Airflow Services

```bash
# First, stop and restart Airflow
cd /workspace
docker-compose -f orchestration/docker-compose.yml down
docker-compose -f orchestration/docker-compose.yml up -d
```

## Step 2: Copy DAG Files to Airflow Container

Since we're using bind mounts, we need to ensure the DAG files are accessible:

```bash
# Create the logs and plugins directories if they don't exist
mkdir -p orchestration/logs
mkdir -p orchestration/plugins

# Restart just the Airflow services
docker-compose -f orchestration/docker-compose.yml restart airflow-webserver airflow-scheduler airflow-worker
```

## Step 3: Check Airflow Scheduler Logs

```bash
# Check scheduler logs for any DAG parsing errors
docker logs orchestration-airflow-scheduler-1 --tail 100
```

## Step 4: Manually Trigger DAG Refresh

```bash
# Access the Airflow scheduler container
docker exec -it orchestration-airflow-scheduler-1 /bin/bash

# Inside the container, check if DAGs are visible
ls -la /opt/airflow/dags/

# Exit the container
exit
```

## Step 5: Complete Fix - Restart Everything

If the above steps don't work, do a complete restart:

```bash
# Stop all services
make down

# Start services again
make start

# Wait 60 seconds for everything to initialize
sleep 60

# Check Airflow UI again at http://localhost:8080
```

## Step 6: Alternative - Use Docker Exec to Copy DAGs

```bash
# Copy DAG files directly into the running containers
docker cp orchestration/dags/. orchestration-airflow-scheduler-1:/opt/airflow/dags/
docker cp orchestration/dags/. orchestration-airflow-webserver-1:/opt/airflow/dags/
docker cp orchestration/dags/. orchestration-airflow-worker-1:/opt/airflow/dags/

# Restart the scheduler to pick up the DAGs
docker restart orchestration-airflow-scheduler-1
```

## Step 7: Check for DAG Errors

If DAGs still don't appear, they might have syntax errors:

```bash
# Test DAG files for errors
docker exec orchestration-airflow-scheduler-1 python /opt/airflow/dags/create_iceberg_tables_dag.py
docker exec orchestration-airflow-scheduler-1 python /opt/airflow/dags/bronze_to_silver_etl_dag.py
docker exec orchestration-airflow-scheduler-1 python /opt/airflow/dags/batch_etl_dag.py
```

## Quick Fix Script

Create and run this script:

```bash
#!/bin/bash
echo "🔧 Fixing Airflow DAGs..."

# Ensure directories exist
mkdir -p orchestration/logs
mkdir -p orchestration/plugins

# Restart Airflow services
echo "🔄 Restarting Airflow services..."
cd orchestration
docker-compose restart
cd ..

# Wait for services to be ready
echo "⏳ Waiting for Airflow to initialize (30 seconds)..."
sleep 30

# Check if scheduler is running
echo "📊 Checking Airflow scheduler status..."
docker logs orchestration-airflow-scheduler-1 --tail 20

echo "✅ Done! Check http://localhost:8080"
echo "   Username: admin"
echo "   Password: admin"
```

## If Nothing Works - Nuclear Option

```bash
# Complete cleanup and restart
make down
docker volume prune -f
docker network create bakery-network 2>/dev/null || true
make start

# After services are up, manually trigger DAG discovery
docker exec orchestration-airflow-scheduler-1 airflow dags list
```

## Expected Result

After following these steps, you should see these DAGs in Airflow UI:
- create_iceberg_tables_dag
- bronze_to_silver_etl_dag  
- batch_etl_dag
- data_quality_dag
- streaming_dag