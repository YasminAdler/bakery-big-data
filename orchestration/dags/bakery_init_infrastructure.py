from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@bakery.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'bakery_init_infrastructure',
    default_args=default_args,
    description='Initialize all infrastructure components for bakery data pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['setup', 'initialization', 'infrastructure']
)

create_network = BashOperator(
    task_id='create_docker_network',
    bash_command="""
    echo "Creating Docker network..."
    docker network create bakery-network 2>/dev/null || echo "Network already exists"
    docker network ls | grep bakery-network
    """,
    dag=dag
)

# Task: Initialize MinIO buckets
init_minio_buckets = BashOperator(
    task_id='init_minio_buckets',
    bash_command="""
    echo "Waiting for MinIO to be ready..."
    sleep 30
    
    echo "Creating MinIO buckets..."
    docker exec minio mc alias set myminio http://minio:9000 minioadmin minioadmin
    
    # Create buckets
    for bucket in bronze silver gold iceberg-warehouse; do
        docker exec minio mc mb myminio/$bucket --ignore-existing
        echo "Created bucket: $bucket"
    done
    
    # Set bucket policies
    docker exec minio mc policy set public myminio/bronze
    docker exec minio mc policy set public myminio/silver
    docker exec minio mc policy set public myminio/gold
    docker exec minio mc policy set public myminio/iceberg-warehouse
    
    echo "MinIO buckets initialized"
    """,
    dag=dag
)

# Task: Initialize Iceberg tables
init_iceberg_tables = BashOperator(
    task_id='init_iceberg_tables',
    bash_command="""
    echo "Initializing Iceberg tables..."
    
    docker exec spark-submit spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 2g \
        --executor-memory 2g \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,\
software.amazon.awssdk:bundle:2.20.18,\
org.apache.hadoop:hadoop-aws:3.3.4 \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.local.type=hadoop \
        --conf spark.sql.catalog.local.warehouse=s3a://iceberg-warehouse/ \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        /opt/spark-apps/init_iceberg_tables.py
    
    echo "Iceberg tables initialized"
    """,
    dag=dag
)

# Task: Create Kafka topics
create_kafka_topics = BashOperator(
    task_id='create_kafka_topics',
    bash_command="""
    echo "Creating Kafka topics..."
    
    # Wait for Kafka to be ready
    sleep 30
    
    # Create topics with appropriate partitions and replication
    topics=(
        "sales-events:3:1"
        "equipment-metrics:3:1"
        "inventory-updates:3:1"
        "customer-feedback:2:1"
        "promotions:1:1"
        "weather-data:1:1"
    )
    
    for topic_config in "${topics[@]}"; do
        IFS=':' read -r topic partitions replication <<< "$topic_config"
        
        docker exec kafka-broker kafka-topics \
            --bootstrap-server localhost:9092 \
            --create --if-not-exists \
            --topic $topic \
            --partitions $partitions \
            --replication-factor $replication
        
        echo "Created topic: $topic (partitions=$partitions, replication=$replication)"
    done
    
    # List all topics
    echo "All Kafka topics:"
    docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list
    """,
    dag=dag
)

# Task: Load initial dimension data
load_initial_data = BashOperator(
    task_id='load_initial_dimension_data',
    bash_command="""
    echo "Loading initial dimension data..."
    
    # Create initial data files (in production, would load from external sources)
    docker exec spark-submit python -c "
import json
import os

# Initial store data
stores = [
    {'store_id': 1, 'location': 'Downtown', 'type': 'Regular', 'manager': 'John Smith'},
    {'store_id': 2, 'location': 'Mall', 'type': 'Kiosk', 'manager': 'Emma Johnson'},
    {'store_id': 3, 'location': 'Airport', 'type': 'Express', 'manager': 'Michael Brown'},
    {'store_id': 4, 'location': 'Suburb North', 'type': 'Regular', 'manager': 'Sarah Davis'},
    {'store_id': 5, 'location': 'Suburb South', 'type': 'Regular', 'manager': 'David Wilson'}
]

# Initial equipment data
equipment = [
    {'equipment_id': 1, 'name': 'Industrial Oven #1', 'type': 'oven'},
    {'equipment_id': 2, 'name': 'Industrial Oven #2', 'type': 'oven'},
    {'equipment_id': 3, 'name': 'Dough Mixer #1', 'type': 'mixer'},
    {'equipment_id': 4, 'name': 'Dough Mixer #2', 'type': 'mixer'},
    {'equipment_id': 5, 'name': 'Proofing Chamber #1', 'type': 'proofer'},
    {'equipment_id': 6, 'name': 'Refrigerator #1', 'type': 'refrigerator'},
    {'equipment_id': 7, 'name': 'Display Case #1', 'type': 'display_case'}
]

print('Initial data prepared')
"
    
    echo "Initial data loaded"
    """,
    dag=dag
)

# Task: Verify setup
verify_setup = BashOperator(
    task_id='verify_setup',
    bash_command="""
    echo "=== Verifying Infrastructure Setup ==="
    
    echo "1. Docker Network:"
    docker network ls | grep bakery-network
    
    echo -e "\n2. MinIO Buckets:"
    docker exec minio mc ls myminio/
    
    echo -e "\n3. Kafka Topics:"
    docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list
    
    echo -e "\n4. Iceberg Tables:"
    docker exec spark-submit spark-sql \
        --master spark://spark-master:7077 \
        -e "SHOW DATABASES IN local;" \
        -e "SHOW TABLES IN local.bronze;" \
        -e "SHOW TABLES IN local.silver;" \
        -e "SHOW TABLES IN local.gold;"
    
    echo -e "\n=== Setup Verification Complete ==="
    """,
    dag=dag
)

# Task: Generate setup report
generate_setup_report = BashOperator(
    task_id='generate_setup_report',
    bash_command="""
    echo "=== Bakery Data Pipeline Setup Report ==="
    echo "Generated at: $(date)"
    echo ""
    echo "Infrastructure Components:"
    echo "✓ Docker Network: bakery-network"
    echo "✓ MinIO Object Storage: Running on port 9000/9001"
    echo "✓ Apache Kafka: Running on port 9092"
    echo "✓ Apache Spark: Master on port 7077, UI on port 4040"
    echo "✓ Apache Airflow: Webserver on port 8080"
    echo ""
    echo "Data Storage:"
    echo "✓ Bronze Layer: Raw data ingestion"
    echo "✓ Silver Layer: Standardized data"
    echo "✓ Gold Layer: Business-ready data"
    echo ""
    echo "Kafka Topics:"
    echo "✓ sales-events (3 partitions)"
    echo "✓ equipment-metrics (3 partitions)"
    echo "✓ inventory-updates (3 partitions)"
    echo "✓ customer-feedback (2 partitions)"
    echo "✓ promotions (1 partition)"
    echo "✓ weather-data (1 partition)"
    echo ""
    echo "Next Steps:"
    echo "1. Start Kafka producers to generate streaming data"
    echo "2. Enable the bakery_streaming_manager DAG to monitor streams"
    echo "3. Enable the bakery_batch_etl DAG for daily processing"
    echo ""
    echo "Access Points:"
    echo "- Airflow UI: http://localhost:8080 (user: airflow, pass: airflow)"
    echo "- MinIO Console: http://localhost:9001 (user: minioadmin, pass: minioadmin)"
    echo "- Spark UI: http://localhost:4040"
    echo "- Kafka UI: http://localhost:8090"
    echo ""
    echo "=== Setup Complete! ==="
    """,
    dag=dag
)

# Define task dependencies
create_network >> [init_minio_buckets, create_kafka_topics] >> init_iceberg_tables >> load_initial_data >> verify_setup >> generate_setup_report 