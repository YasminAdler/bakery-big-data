"""
DAG for creating and populating Silver Layer Iceberg tables
This DAG creates the standardized tables in the silver layer and triggers ETL jobs to populate them.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
    'silver_layer_etl',
    default_args=default_args,
    description='Create and populate Silver Layer Iceberg tables',
    schedule_interval='@daily',
    catchup=False,
    tags=['silver', 'iceberg', 'etl']
)

def create_silver_tables():
    """Create the silver layer Iceberg tables"""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import *
    
    # Initialize Spark session with Iceberg support
    spark = SparkSession.builder \
        .appName("SilverTablesCreation") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://bakery-warehouse/silver") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    # Create database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS local.db")
    
    # Create Silver Sales table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.db.silver_sales (
            sale_id STRING,
            product_id INT,
            store_id INT, 
            customer_id STRING,
            sale_date DATE,
            sale_time STRING,
            time_of_day STRING,
            quantity_sold INT,
            unit_price DECIMAL(10,2),
            total_revenue DECIMAL(10,2),
            promo_id STRING,
            weather_id STRING,
            data_quality_score INT,
            source_system STRING,
            etl_timestamp TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (sale_date)
    """)
    
    # Create Silver Inventory table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.db.silver_inventory (
            inventory_id STRING,
            product_id INT,
            store_id INT,
            inventory_date DATE,
            beginning_stock INT,
            restocked_quantity INT,
            sold_quantity INT,
            waste_quantity INT,
            waste_ratio DECIMAL(5,4),
            closing_stock INT,
            days_of_supply DECIMAL(5,2),
            data_quality_score INT,
            source_system STRING,
            etl_timestamp TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (inventory_date)
    """)
    
    # Create Silver Customer Feedback table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.db.silver_customer_feedback (
            feedback_id STRING,
            customer_id STRING,
            product_id INT,
            feedback_date DATE,
            platform STRING,
            rating INT,
            review_text STRING,
            sentiment_category STRING,
            source_system STRING,
            etl_timestamp TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (feedback_date)
    """)
    
    # Create Silver Equipment Metrics table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.db.silver_equipment_metrics (
            metric_id STRING,
            equipment_id INT,
            metric_date DATE,
            metric_time STRING,
            power_consumption DECIMAL(8,2),
            operational_status STRING,
            operational_hours DECIMAL(5,2),
            maintenance_alert BOOLEAN,
            source_system STRING,
            etl_timestamp TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (metric_date)
    """)
    
    print("✅ All Silver layer tables created successfully!")
    spark.stop()

# Task to create tables
create_tables_task = PythonOperator(
    task_id='create_silver_tables',
    python_callable=create_silver_tables,
    dag=dag
)

# Task to run the ETL job
run_silver_etl = SparkSubmitOperator(
    task_id='run_silver_etl',
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

# Task dependencies
create_tables_task >> run_silver_etl 