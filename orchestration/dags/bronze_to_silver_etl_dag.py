from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Set up logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'bakery-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition
with DAG(
    dag_id='bronze_to_silver_etl_dag',
    default_args=default_args,
    description='ETL pipeline to process data from Bronze to Silver layer',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'bronze', 'silver']
) as dag:

    # Task to process Bronze to Silver ETL
    bronze_to_silver_etl = SparkSubmitOperator(
        task_id='bronze_to_silver_etl',
        application='/opt/spark/jobs/ETL/bronze_to_silver.py',
        name='bronze_to_silver_etl',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.executor.memory': '2g',
            'spark.executor.cores': '2',
            'spark.driver.memory': '2g',
            'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
            'spark.sql.catalog.spark_catalog.type': 'hive',
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.access.key': 'minioadmin',
            'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.sql.warehouse.dir': 's3a://warehouse',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true'
        },
        dag=dag
    )

    # Task to show ETL summary
    show_etl_summary = PythonOperator(
        task_id='show_etl_summary',
        python_callable=lambda: logger.info("""
        ✅ Bronze to Silver ETL completed successfully!
        
        📊 Processed tables:
        - silver_sales: POS transactions with data quality scores
        - silver_inventory: Inventory updates with waste metrics
        - silver_customer_feedback: Customer reviews with sentiment
        - silver_equipment_metrics: Equipment performance data
        
        📁 Output location: s3a://silver/
        """),
        dag=dag
    )

    # Set dependencies
    bronze_to_silver_etl >> show_etl_summary