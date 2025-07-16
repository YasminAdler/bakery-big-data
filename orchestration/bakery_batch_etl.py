"""
Bakery Batch ETL DAG
Daily batch processing pipeline for Bronze → Silver → Gold layers
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import os

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
    'bakery_batch_etl',
    default_args=default_args,
    description='Daily batch ETL pipeline for bakery data',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily
    catchup=False,
    tags=['etl', 'batch', 'daily']
)

# Spark submit command template
SPARK_SUBMIT_CMD = """
docker exec spark-submit spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 2 \
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
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    /opt/spark-apps/{job_file} {job_args}
"""


def check_data_quality_threshold(**context):
    """Check if data quality meets minimum threshold"""
    # In production, this would query the quality metrics from the database
    # For now, we'll simulate a quality check
    quality_score = 85  # Simulated score
    threshold = 80
    
    if quality_score < threshold:
        raise ValueError(f"Data quality score {quality_score} is below threshold {threshold}")
    
    context['task_instance'].xcom_push(key='quality_score', value=quality_score)
    return True


# Task: Check source data availability
check_source_data = BashOperator(
    task_id='check_source_data',
    bash_command="""
    echo "Checking source data availability..."
    # In production, would check Kafka topics, MinIO buckets, etc.
    echo "Source data check completed"
    """,
    dag=dag
)

# Task: Generate batch data (for demo purposes)
generate_batch_data = BashOperator(
    task_id='generate_batch_data',
    bash_command="""
    echo "Generating batch data for processing..."
    docker exec kafka-producer python /app/generate_sales_events.py &
    docker exec kafka-producer python /app/generate_inventory_updates.py &
    docker exec kafka-producer python /app/generate_equipment_metrics.py &
    sleep 60  # Let it generate some data
    pkill -f generate_
    echo "Batch data generation completed"
    """,
    dag=dag
)

# Task Group: Bronze to Silver Processing
with TaskGroup("bronze_to_silver", dag=dag) as bronze_to_silver:
    
    process_bronze_to_silver = BashOperator(
        task_id='process_bronze_to_silver',
        bash_command=SPARK_SUBMIT_CMD.format(
            job_file="bronze_to_silver.py",
            job_args="{{ ds }}"  # Pass execution date
        ),
        dag=dag
    )
    
    validate_silver_data = PythonOperator(
        task_id='validate_silver_data',
        python_callable=check_data_quality_threshold,
        dag=dag
    )
    
    process_bronze_to_silver >> validate_silver_data

# Task Group: Silver to Gold Processing
with TaskGroup("silver_to_gold", dag=dag) as silver_to_gold:
    
    process_silver_to_gold = BashOperator(
        task_id='process_silver_to_gold',
        bash_command=SPARK_SUBMIT_CMD.format(
            job_file="silver_to_gold.py",
            job_args="{{ ds }}"
        ),
        dag=dag
    )
    
    update_ml_features = BashOperator(
        task_id='update_ml_features',
        bash_command=SPARK_SUBMIT_CMD.format(
            job_file="update_ml_features.py",
            job_args="{{ ds }}"
        ),
        dag=dag
    )
    
    process_silver_to_gold >> update_ml_features

# Task: Generate daily reports
generate_reports = BashOperator(
    task_id='generate_reports',
    bash_command="""
    echo "Generating daily reports..."
    docker exec spark-submit spark-sql \
        --master spark://spark-master:7077 \
        -e "SELECT COUNT(*) as total_sales FROM local.gold.fact_sales WHERE date = '{{ ds }}'" \
        -e "SELECT COUNT(*) as total_inventory FROM local.gold.fact_inventory WHERE date = '{{ ds }}'"
    echo "Reports generated successfully"
    """,
    dag=dag
)

# Task: Data quality summary
data_quality_summary = BashOperator(
    task_id='data_quality_summary',
    bash_command="""
    echo "Data Quality Summary for {{ ds }}:"
    echo "Bronze Layer: {{ ti.xcom_pull(task_ids='bronze_to_silver.validate_silver_data', key='quality_score') }}%"
    echo "Processing completed successfully"
    """,
    dag=dag
)

# Task: Cleanup old data
cleanup_old_data = BashOperator(
    task_id='cleanup_old_data',
    bash_command="""
    echo "Cleaning up old data..."
    # In production, would implement data retention policies
    # For example, archive Bronze data older than 7 days
    echo "Cleanup completed"
    """,
    dag=dag
)

# Define task dependencies
check_source_data >> generate_batch_data >> bronze_to_silver >> silver_to_gold >> [generate_reports, data_quality_summary] >> cleanup_old_data 