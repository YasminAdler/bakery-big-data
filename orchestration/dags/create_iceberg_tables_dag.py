from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'bakery-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'create_iceberg_tables',
    default_args=default_args,
    description='Initialize Iceberg tables for bakery data warehouse',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['initialization', 'iceberg', 'tables', 'bakery']
)

def check_spark_connectivity(**context):
    """Check if Spark cluster is available"""
    print("Checking Spark connectivity...")
    # Add any pre-flight checks here
    return True

# Pre-flight checks
check_spark = PythonOperator(
    task_id='check_spark_connectivity',
    python_callable=check_spark_connectivity,
    dag=dag
)

# Create database and bronze layer tables
create_bronze_tables = SparkSubmitOperator(
    task_id='create_bronze_tables',
    application='/opt/spark/jobs/batch/create_iceberg_tables.py',
    conn_id='spark_default',
    conf={
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        'spark.sql.catalog.spark_catalog.type': 'hive',
        'spark.sql.catalog.local': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.local.type': 'hadoop',
        'spark.sql.catalog.local.warehouse': 's3a://bakery-warehouse/iceberg',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true'
    },
    application_args=['bronze'],
    dag=dag
)

# Create silver layer tables
create_silver_tables = SparkSubmitOperator(
    task_id='create_silver_tables',
    application='/opt/spark/jobs/batch/create_iceberg_tables.py',
    conn_id='spark_default',
    conf={
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        'spark.sql.catalog.spark_catalog.type': 'hive',
        'spark.sql.catalog.local': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.local.type': 'hadoop',
        'spark.sql.catalog.local.warehouse': 's3a://bakery-warehouse/iceberg',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true'
    },
    application_args=['silver'],
    dag=dag
)

# Create gold layer tables
create_gold_tables = SparkSubmitOperator(
    task_id='create_gold_tables',
    application='/opt/spark/jobs/batch/create_iceberg_tables.py',
    conn_id='spark_default',
    conf={
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        'spark.sql.catalog.spark_catalog.type': 'hive',
        'spark.sql.catalog.local': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.local.type': 'hadoop',
        'spark.sql.catalog.local.warehouse': 's3a://bakery-warehouse/iceberg',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true'
    },
    application_args=['gold'],
    dag=dag
)

# Verify all tables were created successfully
verify_tables = SparkSubmitOperator(
    task_id='verify_table_creation',
    application='/opt/spark/jobs/batch/create_iceberg_tables.py',
    conn_id='spark_default',
    conf={
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        'spark.sql.catalog.spark_catalog.type': 'hive',
        'spark.sql.catalog.local': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.local.type': 'hadoop',
        'spark.sql.catalog.local.warehouse': 's3a://bakery-warehouse/iceberg',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true'
    },
    application_args=['all'],
    dag=dag
)

# Success notification
def notify_success(**context):
    """Notify that table creation was successful"""
    print("✅ All Iceberg tables created successfully!")
    print("You can now run the batch ETL DAG.")
    return True

success_notification = PythonOperator(
    task_id='notify_success',
    python_callable=notify_success,
    dag=dag
)

# Define task dependencies
check_spark >> create_bronze_tables >> create_silver_tables >> create_gold_tables >> verify_tables >> success_notification