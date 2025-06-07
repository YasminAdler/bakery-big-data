from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    'owner': 'bakery-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'bakery_streaming_pipeline',
    default_args=default_args,
    description='Manage streaming data processing',
    schedule_interval=None,  # Triggered manually or by external events
    catchup=False,
    tags=['streaming', 'real-time', 'bakery']
)

# Start POS stream processing
start_pos_streaming = SparkSubmitOperator(
    task_id='start_pos_stream_processor',
    application='/opt/spark/jobs/streaming/pos_stream_processor.py',
    conn_id='spark_default',
    conf={
        'spark.streaming.stopGracefullyOnShutdown': 'true',
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true'
    },
    total_executor_cores=2,
    executor_memory='2g',
    driver_memory='1g',
    name='pos-stream-processor',
    dag=dag
)

# Start IoT stream processing
start_iot_streaming = SparkSubmitOperator(
    task_id='start_iot_stream_processor',
    application='/opt/spark/jobs/streaming/iot_stream_processor.py',
    conn_id='spark_default',
    conf={
        'spark.streaming.stopGracefullyOnShutdown': 'true',
        'spark.sql.adaptive.enabled': 'true'
    },
    total_executor_cores=2,
    executor_memory='2g',
    driver_memory='1g',
    name='iot-stream-processor',
    dag=dag
)

# Monitor streaming health
check_streaming_health = BashOperator(
    task_id='check_streaming_health',
    bash_command="""
    # Check if streaming jobs are running
    curl -X GET http://spark-master:8080/api/v1/applications | \
    jq '.[] | select(.name | contains("stream-processor")) | {name, state}'
    """,
    dag=dag
)

# Dependencies
[start_pos_streaming, start_iot_streaming] >> check_streaming_health