from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'bakery-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'bakery_batch_etl',
    default_args=default_args,
    description='Batch ETL pipeline for bakery data warehouse',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['batch', 'etl', 'bakery']
)

# Data quality check function
def check_data_quality(**context):
    # Implementation for data quality checks
    pass

# Task Groups for Bronze to Silver ETL
with TaskGroup("bronze_to_silver", dag=dag) as bronze_to_silver:
    
    check_bronze_quality = PythonOperator(
        task_id='check_bronze_data_quality',
        python_callable=check_data_quality,
        op_kwargs={'layer': 'bronze'}
    )
    
    process_sales = SparkSubmitOperator(
        task_id='process_sales_bronze_to_silver',
        application='/opt/spark/jobs/batch/bronze_to_silver.py',
        conn_id='spark_default',
        conf={
            'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
            'spark.sql.catalog.spark_catalog.type': 'hive',
            'spark.sql.catalog.local': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.local.type': 'hadoop',
            'spark.sql.catalog.local.warehouse': 's3a://bakery-warehouse/iceberg'
        },
        application_args=['sales', '{{ ds }}']
    )
    
    process_inventory = SparkSubmitOperator(
        task_id='process_inventory_bronze_to_silver',
        application='/opt/spark/jobs/batch/bronze_to_silver.py',
        conn_id='spark_default',
        application_args=['inventory', '{{ ds }}']
    )
    
    process_equipment = SparkSubmitOperator(
        task_id='process_equipment_bronze_to_silver',
        application='/opt/spark/jobs/batch/bronze_to_silver.py',
        conn_id='spark_default',
        application_args=['equipment', '{{ ds }}']
    )
    
    check_bronze_quality >> [process_sales, process_inventory, process_equipment]

# Task Groups for Silver to Gold ETL
with TaskGroup("silver_to_gold", dag=dag) as silver_to_gold:
    
    check_silver_quality = PythonOperator(
        task_id='check_silver_data_quality',
        python_callable=check_data_quality,
        op_kwargs={'layer': 'silver'}
    )
    
    update_dimensions = SparkSubmitOperator(
        task_id='update_dimension_tables',
        application='/opt/spark/jobs/batch/silver_to_gold.py',
        conn_id='spark_default',
        application_args=['dimensions', '{{ ds }}']
    )
    
    update_facts = SparkSubmitOperator(
        task_id='update_fact_tables',
        application='/opt/spark/jobs/batch/silver_to_gold.py',
        conn_id='spark_default',
        application_args=['facts', '{{ ds }}']
    )
    
    update_ml_features = SparkSubmitOperator(
        task_id='update_ml_feature_tables',
        application='/opt/spark/jobs/batch/silver_to_gold.py',
        conn_id='spark_default',
        application_args=['ml_features', '{{ ds }}']
    )
    
    check_silver_quality >> update_dimensions >> [update_facts, update_ml_features]

# Handle late-arriving data
handle_late_arrivals = SparkSubmitOperator(
    task_id='handle_late_arriving_data',
    application='/opt/spark/jobs/batch/late_arrival_handler.py',
    conn_id='spark_default',
    application_args=['{{ ds }}', '48']  # 48 hours window
)

# Data quality report
generate_quality_report = PythonOperator(
    task_id='generate_data_quality_report',
    python_callable=check_data_quality,
    op_kwargs={'generate_report': True}
)

# Define dependencies
bronze_to_silver >> silver_to_gold >> handle_late_arrivals >> generate_quality_report