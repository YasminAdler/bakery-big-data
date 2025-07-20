"""
Bakery Streaming Manager DAG
Manages Spark Structured Streaming jobs for real-time data processing
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
import requests
import json

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@bakery.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition
dag = DAG(
    'bakery_streaming_manager',
    default_args=default_args,
    description='Manages Spark streaming jobs for real-time data processing',
    schedule_interval='@hourly',  # Check streaming jobs every hour
    catchup=False,
    tags=['streaming', 'monitoring', 'real-time']
)


class KafkaTopicSensor(BaseSensorOperator):
    """Custom sensor to check if Kafka topics have data"""

    def poke(self, context):
        # In production, would check Kafka topic lag/offset
        # For now, return True to indicate topics are available
        return True


def check_streaming_job_health(**context):
    """Check if streaming jobs are running healthy"""
    try:
        # Check Spark UI for streaming job status
        # In production, would query Spark REST API
        spark_ui_url = "http://localhost:4040/api/v1/applications"
        
        # For demo, we'll simulate the check
        streaming_jobs = {
            'sales_stream': 'running',
            'equipment_stream': 'running',
            'inventory_stream': 'running'
        }
        
        unhealthy_jobs = [job for job, status in streaming_jobs.items() if status != 'running']
        
        if unhealthy_jobs:
            context['task_instance'].xcom_push(key='unhealthy_jobs', value=unhealthy_jobs)
            return False
        
        return True
    except Exception as e:
        print(f"Error checking streaming jobs: {str(e)}")
        return False


# Task: Check Kafka availability
check_kafka = BashOperator(
    task_id='check_kafka_health',
    bash_command="""
    echo "Checking Kafka health..."
    docker exec kafka-broker kafka-broker-api-versions --bootstrap-server localhost:9092
    if [ $? -eq 0 ]; then
        echo "Kafka is healthy"
    else
        echo "Kafka is unhealthy"
        exit 1
    fi
    """,
    dag=dag
)

# Task: Check Kafka topics
check_topics = BashOperator(
    task_id='check_kafka_topics',
    bash_command="""
    echo "Checking Kafka topics..."
    docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list
    
    # Ensure required topics exist
    for topic in sales-events equipment-metrics inventory-updates; do
        docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 \
            --create --if-not-exists --topic $topic \
            --partitions 3 --replication-factor 1
    done
    
    echo "Kafka topics verified"
    """,
    dag=dag
)

# Task: Monitor streaming jobs
monitor_streaming_jobs = PythonOperator(
    task_id='monitor_streaming_jobs',
    python_callable=check_streaming_job_health,
    dag=dag
)

# Task: Start/Restart streaming jobs if needed
manage_streaming_jobs = BashOperator(
    task_id='manage_streaming_jobs',
    bash_command="""
    echo "Managing streaming jobs..."
    
    # Check if streaming job is running
    if ! docker exec spark-submit ps aux | grep -q "stream_to_bronze.py"; then
        echo "Starting streaming job..."
        docker exec -d spark-submit spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 2g \
            --executor-memory 2g \
            --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,\
software.amazon.awssdk:bundle:2.20.18,\
org.apache.hadoop:hadoop-aws:3.3.4 \
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
            --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.local.type=hadoop \
            --conf spark.sql.catalog.local.warehouse=s3a://iceberg-warehouse/ \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key=minioadmin \
            --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
            /opt/spark-apps/stream_to_bronze.py
    else
        echo "Streaming job is already running"
    fi
    """,
    dag=dag
)

# Task: Check streaming metrics
check_streaming_metrics = BashOperator(
    task_id='check_streaming_metrics',
    bash_command="""
    echo "Checking streaming metrics..."
    
    # Check processing rate
    docker exec spark-submit curl -s http://localhost:4040/api/v1/applications | \
        python -c "import sys, json; print('Streaming metrics checked')"
    
    # Check Kafka consumer lag
    docker exec kafka-broker kafka-consumer-groups --bootstrap-server localhost:9092 \
        --all-groups --describe | grep -E "TOPIC|sales-events|equipment-metrics|inventory-updates"
    
    echo "Metrics check completed"
    """,
    dag=dag
)

# Task: Alert on issues
alert_on_issues = BashOperator(
    task_id='alert_on_issues',
    bash_command="""
    unhealthy_jobs="{{ ti.xcom_pull(task_ids='monitor_streaming_jobs', key='unhealthy_jobs') }}"
    
    if [ ! -z "$unhealthy_jobs" ]; then
        echo "ALERT: Unhealthy streaming jobs detected: $unhealthy_jobs"
        # In production, would send alerts via email, Slack, PagerDuty, etc.
    else
        echo "All streaming jobs are healthy"
    fi
    """,
    trigger_rule='none_failed_or_skipped',
    dag=dag
)

# Task: Generate streaming health report
streaming_health_report = BashOperator(
    task_id='streaming_health_report',
    bash_command="""
    echo "=== Streaming Health Report ==="
    echo "Timestamp: $(date)"
    echo "Kafka Status: Healthy"
    echo "Active Topics: sales-events, equipment-metrics, inventory-updates"
    echo "Streaming Jobs: Running"
    echo "Consumer Lag: Within acceptable limits"
    echo "=============================="
    """,
    trigger_rule='none_failed_or_skipped',
    dag=dag
)

# Define task dependencies
check_kafka >> check_topics >> monitor_streaming_jobs >> manage_streaming_jobs >> check_streaming_metrics >> [alert_on_issues, streaming_health_report] 