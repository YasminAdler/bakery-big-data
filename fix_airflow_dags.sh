#!/bin/bash

echo "🔧 Fixing Airflow DAGs visibility issue..."
echo "============================================"

# Step 1: Ensure directories exist
echo "📁 Creating required directories..."
mkdir -p orchestration/logs
mkdir -p orchestration/plugins

# Step 2: Check if Airflow is running
echo "🔍 Checking if Airflow is running..."
if ! docker ps | grep -q "airflow"; then
    echo "❌ Airflow is not running. Starting services..."
    make start
    echo "⏳ Waiting 60 seconds for services to start..."
    sleep 60
fi

# Step 3: Get container names
SCHEDULER_CONTAINER=$(docker ps --format "table {{.Names}}" | grep -E "scheduler" | head -1)
WEBSERVER_CONTAINER=$(docker ps --format "table {{.Names}}" | grep -E "webserver" | head -1)
WORKER_CONTAINER=$(docker ps --format "table {{.Names}}" | grep -E "worker" | head -1)

echo "📦 Found containers:"
echo "  - Scheduler: $SCHEDULER_CONTAINER"
echo "  - Webserver: $WEBSERVER_CONTAINER"
echo "  - Worker: $WORKER_CONTAINER"

# Step 4: Copy DAG files to containers
echo "📋 Copying DAG files to Airflow containers..."
if [ -n "$SCHEDULER_CONTAINER" ]; then
    docker cp orchestration/dags/. $SCHEDULER_CONTAINER:/opt/airflow/dags/
fi
if [ -n "$WEBSERVER_CONTAINER" ]; then
    docker cp orchestration/dags/. $WEBSERVER_CONTAINER:/opt/airflow/dags/
fi
if [ -n "$WORKER_CONTAINER" ]; then
    docker cp orchestration/dags/. $WORKER_CONTAINER:/opt/airflow/dags/
fi

# Step 5: Restart scheduler to pick up DAGs
echo "🔄 Restarting Airflow scheduler..."
if [ -n "$SCHEDULER_CONTAINER" ]; then
    docker restart $SCHEDULER_CONTAINER
fi

# Step 6: Wait for scheduler to initialize
echo "⏳ Waiting 20 seconds for scheduler to initialize..."
sleep 20

# Step 7: List DAGs in scheduler
echo "📊 Checking DAGs in scheduler..."
if [ -n "$SCHEDULER_CONTAINER" ]; then
    docker exec $SCHEDULER_CONTAINER airflow dags list
fi

# Step 8: Show final instructions
echo ""
echo "✅ Fix applied! Please check:"
echo "   1. Open http://localhost:8080"
echo "   2. Login with username: admin, password: admin"
echo "   3. You should see the following DAGs:"
echo "      - create_iceberg_tables_dag"
echo "      - bronze_to_silver_etl_dag"
echo "      - batch_etl_dag"
echo "      - data_quality_dag"
echo "      - streaming_dag"
echo ""
echo "If DAGs still don't appear, run: make down && make start"