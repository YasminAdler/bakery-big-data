"""
Complete Data Pipeline DAG
Orchestrates the entire data pipeline from Bronze to Gold layers.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
import os

# Default arguments
default_args = {
    'owner': 'bakery-data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'complete_data_pipeline',
    default_args=default_args,
    description='Complete data pipeline from Bronze to Gold layers',
    schedule_interval='@daily',
    catchup=False,
    tags=['pipeline', 'bronze', 'silver', 'gold', 'etl']
)

# Start task
start = DummyOperator(task_id='start_pipeline', dag=dag)

# Create Silver tables
create_silver_tables = SparkSubmitOperator(
    task_id='create_silver_tables',
    application='/opt/spark-apps/etl/create_silver_tables.py',
    conn_id='spark_default',
    conf={
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.local': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.local.type': 'hadoop',
        'spark.sql.catalog.local.warehouse': 's3a://bakery-warehouse/silver',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'
    },
    packages='org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2',
    dag=dag
)

# Bronze to Silver ETL
bronze_to_silver = SparkSubmitOperator(
    task_id='bronze_to_silver_etl',
    application='/opt/spark-apps/etl/bronze_to_silver.py',
    conn_id='spark_default',
    conf={
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.local': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.local.type': 'hadoop',
        'spark.sql.catalog.local.warehouse': 's3a://bakery-warehouse/silver',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'
    },
    packages='org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2',
    dag=dag
)

# Create Gold tables
create_gold_tables = SparkSubmitOperator(
    task_id='create_gold_tables',
    application='/opt/spark-apps/etl/create_gold_tables.py',
    conn_id='spark_default',
    conf={
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.local': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.local.type': 'hadoop',
        'spark.sql.catalog.local.warehouse': 's3a://bakery-warehouse/gold',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'
    },
    packages='org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2',
    dag=dag
)

# Silver to Gold ETL
silver_to_gold = SparkSubmitOperator(
    task_id='silver_to_gold_etl',
    application='/opt/spark-apps/etl/silver_to_gold.py',
    conn_id='spark_default',
    conf={
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.local': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.local.type': 'hadoop',
        'spark.sql.catalog.local.warehouse': 's3a://bakery-warehouse/gold',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'
    },
    packages='org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2',
    dag=dag
)

# Data Quality Check
def data_quality_check():
    """Perform data quality checks on the final gold layer"""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("DataQualityCheck") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://bakery-warehouse/gold") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    # Check fact_sales
    sales_count = spark.read.format("iceberg").load("local.gold.fact_sales").count()
    print(f"✅ Fact sales records: {sales_count}")
    
    # Check fact_inventory
    inventory_count = spark.read.format("iceberg").load("local.gold.fact_inventory").count()
    print(f"✅ Fact inventory records: {inventory_count}")
    
    # Check fact_customer_feedback
    feedback_count = spark.read.format("iceberg").load("local.gold.fact_customer_feedback").count()
    print(f"✅ Fact feedback records: {feedback_count}")
    
    # Check fact_equipment_performance
    equipment_count = spark.read.format("iceberg").load("local.gold.fact_equipment_performance").count()
    print(f"✅ Fact equipment records: {equipment_count}")
    
    # Check dimensions
    product_count = spark.read.format("iceberg").load("local.gold.dim_product").count()
    store_count = spark.read.format("iceberg").load("local.gold.dim_store").count()
    equipment_count_dim = spark.read.format("iceberg").load("local.gold.dim_equipment").count()
    
    print(f"✅ Dimension records - Products: {product_count}, Stores: {store_count}, Equipment: {equipment_count_dim}")
    
    spark.stop()
    print("🎉 Data quality check completed successfully!")

data_quality = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag
)

# End task
end = DummyOperator(task_id='end_pipeline', dag=dag)

# Define task dependencies
start >> create_silver_tables >> bronze_to_silver >> create_gold_tables >> silver_to_gold >> data_quality >> end 