"""
Bakery CSV ETL Pipeline - Manual Trigger DAG
Simple manual trigger for immediate CSV processing when new files are available
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,  # Disable email to avoid connection errors
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# DAG definition
dag = DAG(
    'bakery_csv_manual_trigger',
    default_args=default_args,
    description='Manual trigger for CSV ETL pipeline - Run on demand',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['csv', 'manual', 'etl', 'trigger']
)

def pre_pipeline_check(**context):
    """Quick pre-pipeline validation"""
    logging.info("Starting manual CSV ETL pipeline...")
    logging.info(f"DAG Run ID: {context['dag_run'].run_id}")
    logging.info(f"Execution Date: {context['ds']}")
    return True

def post_pipeline_summary(**context):
    """Post-pipeline summary"""
    logging.info("Manual CSV ETL pipeline completed successfully!")
    logging.info("Check the Spark UI and MinIO console for results")
    return True

# Task: Pre-pipeline check
pre_check = PythonOperator(
    task_id='pre_pipeline_check',
    python_callable=pre_pipeline_check,
    dag=dag
)

# Task: Load CSV to Bronze using make command
load_bronze = BashOperator(
    task_id='load_csv_to_bronze',
    bash_command='cd /opt/airflow && make load-bronze',
    dag=dag
)

# Task: Bronze to Silver using make command
bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',
    bash_command='cd /opt/airflow && make batch-etl',
    dag=dag
)

# Task: Show final data counts
show_results = BashOperator(
    task_id='show_results',
    bash_command='cd /opt/airflow && make show-data',
    dag=dag
)

# Task: Post-pipeline summary
post_summary = PythonOperator(
    task_id='post_pipeline_summary',
    python_callable=post_pipeline_summary,
    dag=dag
)

# Define task dependencies - Simple linear flow
pre_check >> load_bronze >> bronze_to_silver >> show_results >> post_summary 