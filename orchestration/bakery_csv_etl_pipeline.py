"""
Bakery CSV ETL Pipeline DAG
Complete pipeline to load CSV data from warehouse bucket through Bronze → Silver → Gold layers
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.utils.dates import days_ago
import logging

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@bakery.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition
dag = DAG(
    'bakery_csv_etl_pipeline',
    default_args=default_args,
    description='Complete CSV ETL pipeline from warehouse bucket to Gold layer',
    schedule_interval=timedelta(hours=1),  # Check every hour for new CSV files
    catchup=False,
    max_active_runs=1,  # Prevent parallel runs
    tags=['csv', 'etl', 'pipeline', 'bronze', 'silver', 'gold']
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
    --conf spark.local.dir=/tmp \
    --conf hadoop.tmp.dir=/tmp \
    --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
    --conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
    /opt/spark-apps/{job_file} {job_args}
"""

def check_csv_file_presence(**context):
    """Check if CSV file exists in the warehouse bucket"""
    try:
        # Check if bronze_combined.csv exists in the container
        import subprocess
        result = subprocess.run([
            'docker', 'exec', 'spark-submit', 
            'ls', '/opt/spark-apps/data/bronze_combined.csv'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            logging.info("CSV file found: bronze_combined.csv")
            return True
        else:
            logging.warning("CSV file not found in expected location")
            return False
    except Exception as e:
        logging.error(f"Error checking CSV file: {str(e)}")
        return False

def validate_infrastructure(**context):
    """Validate that all required infrastructure is available"""
    try:
        import subprocess
        
        # Check MinIO
        result = subprocess.run(['docker', 'exec', 'minio', 'mc', 'admin', 'info', 'myminio'], 
                              capture_output=True)
        if result.returncode != 0:
            raise Exception("MinIO not available")
        
        # Check Spark
        result = subprocess.run(['docker', 'exec', 'spark-submit', 'spark-sql', '--version'], 
                              capture_output=True)
        if result.returncode != 0:
            raise Exception("Spark not available")
        
        # Check Kafka
        result = subprocess.run(['docker', 'exec', 'kafka-broker', 'kafka-topics', 
                               '--bootstrap-server', 'localhost:9092', '--list'], 
                              capture_output=True)
        if result.returncode != 0:
            raise Exception("Kafka not available")
        
        logging.info("All infrastructure components are available")
        return True
    except Exception as e:
        logging.error(f"Infrastructure validation failed: {str(e)}")
        raise

def check_data_quality(**context):
    """Check data quality after each ETL step"""
    try:
        import subprocess
        
        # Check record counts in each layer
        quality_check_sql = """
        SELECT 
            'Bronze Sales Events' AS table_name, 
            COUNT(*) AS record_count,
            COUNT(DISTINCT event_id) AS unique_records
        FROM local.bronze.sales_events
        WHERE processing_status = 'pending'
        
        UNION ALL
        
        SELECT 
            'Bronze Inventory Updates' AS table_name,
            COUNT(*) AS record_count,
            COUNT(DISTINCT update_id) AS unique_records
        FROM local.bronze.inventory_updates
        WHERE processing_status = 'pending'
        
        UNION ALL
        
        SELECT 
            'Bronze Equipment Metrics' AS table_name,
            COUNT(*) AS record_count,
            COUNT(DISTINCT metric_id) AS unique_records
        FROM local.bronze.equipment_metrics
        WHERE processing_status = 'pending'
        """
        
        result = subprocess.run([
            'docker', 'exec', 'spark-submit', 'spark-sql',
            '--packages', 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,software.amazon.awssdk:bundle:2.20.18,org.apache.hadoop:hadoop-aws:3.3.4',
            '--conf', 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            '--conf', 'spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog',
            '--conf', 'spark.sql.catalog.local.type=hadoop',
            '--conf', 'spark.sql.catalog.local.warehouse=s3a://iceberg-warehouse/',
            '--conf', 'spark.hadoop.fs.s3a.endpoint=http://minio:9000',
            '--conf', 'spark.hadoop.fs.s3a.access.key=minioadmin',
            '--conf', 'spark.hadoop.fs.s3a.secret.key=minioadmin',
            '--conf', 'spark.hadoop.fs.s3a.path.style.access=true',
            '-e', quality_check_sql
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            logging.info(f"Data quality check passed: {result.stdout}")
            return True
        else:
            logging.error(f"Data quality check failed: {result.stderr}")
            return False
    except Exception as e:
        logging.error(f"Error during data quality check: {str(e)}")
        return False

def send_pipeline_notification(success=True, **context):
    """Send notification about pipeline completion"""
    try:
        task_instance = context['task_instance']
        dag_run = context['dag_run']
        
        if success:
            message = f"""
            ✅ Bakery CSV ETL Pipeline Completed Successfully!
            
            DAG: {dag_run.dag_id}
            Run ID: {dag_run.run_id}
            Execution Date: {dag_run.execution_date}
            
            Pipeline Steps Completed:
            1. CSV file validation ✅
            2. Infrastructure check ✅
            3. Bronze layer loading ✅
            4. Silver layer processing ✅
            5. Gold layer transformation ✅
            6. ML features update ✅
            7. Data quality validation ✅
            
            Access your data:
            - MinIO Console: http://localhost:9001
            - Spark UI: http://localhost:8080
            - Airflow UI: http://localhost:8081
            """
        else:
            message = f"""
            ❌ Bakery CSV ETL Pipeline Failed!
            
            DAG: {dag_run.dag_id}
            Run ID: {dag_run.run_id}
            Execution Date: {dag_run.execution_date}
            
            Please check the logs for details and retry the pipeline.
            """
        
        logging.info(message)
        # In production, this would send email, Slack, or other notifications
        print(message)
        
    except Exception as e:
        logging.error(f"Error sending notification: {str(e)}")

# Task: Validate infrastructure
validate_infra = PythonOperator(
    task_id='validate_infrastructure',
    python_callable=validate_infrastructure,
    dag=dag
)

# Task: Check CSV file presence
check_csv_presence = PythonOperator(
    task_id='check_csv_file_presence',
    python_callable=check_csv_file_presence,
    dag=dag
)

# Task: Initial data quality check
initial_quality_check = PythonOperator(
    task_id='initial_data_quality_check',
    python_callable=check_data_quality,
    dag=dag
)

# Task Group: Bronze Layer Loading
with TaskGroup("bronze_layer_loading", dag=dag) as bronze_layer:
    
    # Backup existing bronze data (if any)
    backup_bronze_data = BashOperator(
        task_id='backup_bronze_data',
        bash_command="""
        echo "Creating backup of existing bronze data..."
        timestamp=$(date +%Y%m%d_%H%M%S)
        
        # Create backup tables
        docker exec spark-submit spark-sql \
            --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,software.amazon.awssdk:bundle:2.20.18,org.apache.hadoop:hadoop-aws:3.3.4 \
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
            --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.local.type=hadoop \
            --conf spark.sql.catalog.local.warehouse=s3a://iceberg-warehouse/ \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key=minioadmin \
            --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            -e "CREATE TABLE IF NOT EXISTS local.bronze.sales_events_backup_${timestamp} AS SELECT * FROM local.bronze.sales_events;" \
            -e "CREATE TABLE IF NOT EXISTS local.bronze.inventory_updates_backup_${timestamp} AS SELECT * FROM local.bronze.inventory_updates;" \
            -e "CREATE TABLE IF NOT EXISTS local.bronze.equipment_metrics_backup_${timestamp} AS SELECT * FROM local.bronze.equipment_metrics;"
        
        echo "Bronze data backup completed with timestamp: ${timestamp}"
        """,
        dag=dag
    )
    
    # Load CSV data into bronze tables
    load_csv_to_bronze = BashOperator(
        task_id='load_csv_to_bronze',
        bash_command=SPARK_SUBMIT_CMD.format(
            job_file="load_bronze_from_csv.py",
            job_args=""
        ),
        dag=dag
    )
    
    # Validate bronze data loading
    validate_bronze_loading = PythonOperator(
        task_id='validate_bronze_loading',
        python_callable=check_data_quality,
        dag=dag
    )
    
    backup_bronze_data >> load_csv_to_bronze >> validate_bronze_loading

# Task Group: Silver Layer Processing
with TaskGroup("silver_layer_processing", dag=dag) as silver_layer:
    
    # Process Bronze to Silver
    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver_etl',
        bash_command=SPARK_SUBMIT_CMD.format(
            job_file="bronze_to_silver.py",
            job_args="{{ ds }}"  # Pass execution date
        ),
        dag=dag
    )
    
    # Validate Silver data quality
    validate_silver_quality = BashOperator(
        task_id='validate_silver_quality',
        bash_command="""
        echo "Validating Silver layer data quality..."
        
        docker exec spark-submit spark-sql \
            --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,software.amazon.awssdk:bundle:2.20.18,org.apache.hadoop:hadoop-aws:3.3.4 \
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
            --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.local.type=hadoop \
            --conf spark.sql.catalog.local.warehouse=s3a://iceberg-warehouse/ \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key=minioadmin \
            --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            -e "SELECT 'Silver Sales' AS table_name, COUNT(*) AS count, AVG(data_quality_score) AS avg_quality FROM local.silver.sales;" \
            -e "SELECT 'Silver Inventory' AS table_name, COUNT(*) AS count, AVG(data_quality_score) AS avg_quality FROM local.silver.inventory;" \
            -e "SELECT 'Silver Equipment' AS table_name, COUNT(*) AS count FROM local.silver.equipment_metrics;"
        
        echo "Silver layer validation completed"
        """,
        dag=dag
    )
    
    bronze_to_silver >> validate_silver_quality

# Task Group: Gold Layer Processing
with TaskGroup("gold_layer_processing", dag=dag) as gold_layer:
    
    # Process Silver to Gold
    silver_to_gold = BashOperator(
        task_id='silver_to_gold_etl',
        bash_command=SPARK_SUBMIT_CMD.format(
            job_file="silver_to_gold.py",
            job_args="{{ ds }}"
        ),
        dag=dag
    )
    
    # Update ML Features
    update_ml_features = BashOperator(
        task_id='update_ml_features',
        bash_command=SPARK_SUBMIT_CMD.format(
            job_file="update_ml_features.py",
            job_args="{{ ds }}"
        ),
        dag=dag
    )
    
    # Validate Gold layer data
    validate_gold_layer = BashOperator(
        task_id='validate_gold_layer',
        bash_command="""
        echo "Validating Gold layer data..."
        
        docker exec spark-submit spark-sql \
            --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,software.amazon.awssdk:bundle:2.20.18,org.apache.hadoop:hadoop-aws:3.3.4 \
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
            --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.local.type=hadoop \
            --conf spark.sql.catalog.local.warehouse=s3a://iceberg-warehouse/ \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key=minioadmin \
            --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            -e "SELECT 'Gold Fact Sales' AS table_name, COUNT(*) AS count FROM local.gold.fact_sales;" \
            -e "SELECT 'Gold Fact Inventory' AS table_name, COUNT(*) AS count FROM local.gold.fact_inventory;" \
            -e "SELECT 'Gold Fact Equipment' AS table_name, COUNT(*) AS count FROM local.gold.fact_equipment_performance;" \
            -e "SELECT 'Demand Forecast Features' AS table_name, COUNT(*) AS count FROM local.gold.fact_demand_forecast_features;"
        
        echo "Gold layer validation completed"
        """,
        dag=dag
    )
    
    silver_to_gold >> update_ml_features >> validate_gold_layer

# Task: Generate pipeline report
generate_pipeline_report = BashOperator(
    task_id='generate_pipeline_report',
    bash_command="""
    echo "=== Bakery CSV ETL Pipeline Report ==="
    echo "Generated at: $(date)"
    echo "DAG Run ID: {{ dag_run.run_id }}"
    echo "Execution Date: {{ ds }}"
    echo ""
    echo "Data Layer Statistics:"
    
    docker exec spark-submit spark-sql \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,software.amazon.awssdk:bundle:2.20.18,org.apache.hadoop:hadoop-aws:3.3.4 \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.local.type=hadoop \
        --conf spark.sql.catalog.local.warehouse=s3a://iceberg-warehouse/ \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        -e "SELECT 'BRONZE' AS layer, 'Sales Events' AS table_name, COUNT(*) AS records FROM local.bronze.sales_events \
            UNION ALL SELECT 'BRONZE' AS layer, 'Inventory Updates' AS table_name, COUNT(*) AS records FROM local.bronze.inventory_updates \
            UNION ALL SELECT 'BRONZE' AS layer, 'Equipment Metrics' AS table_name, COUNT(*) AS records FROM local.bronze.equipment_metrics \
            UNION ALL SELECT 'SILVER' AS layer, 'Sales' AS table_name, COUNT(*) AS records FROM local.silver.sales \
            UNION ALL SELECT 'SILVER' AS layer, 'Inventory' AS table_name, COUNT(*) AS records FROM local.silver.inventory \
            UNION ALL SELECT 'SILVER' AS layer, 'Equipment Metrics' AS table_name, COUNT(*) AS records FROM local.silver.equipment_metrics \
            UNION ALL SELECT 'GOLD' AS layer, 'Fact Sales' AS table_name, COUNT(*) AS records FROM local.gold.fact_sales \
            UNION ALL SELECT 'GOLD' AS layer, 'Fact Inventory' AS table_name, COUNT(*) AS records FROM local.gold.fact_inventory \
            UNION ALL SELECT 'GOLD' AS layer, 'Fact Equipment Performance' AS table_name, COUNT(*) AS records FROM local.gold.fact_equipment_performance \
            ORDER BY layer, table_name;"
    
    echo ""
    echo "Pipeline completed successfully!"
    echo "================================="
    """,
    dag=dag
)

# Task: Send success notification
send_success_notification = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_pipeline_notification,
    op_kwargs={'success': True},
    trigger_rule='all_success',
    dag=dag
)

# Task: Send failure notification
send_failure_notification = PythonOperator(
    task_id='send_failure_notification',
    python_callable=send_pipeline_notification,
    op_kwargs={'success': False},
    trigger_rule='one_failed',
    dag=dag
)

# Task: Cleanup temporary files
cleanup_temp_files = BashOperator(
    task_id='cleanup_temp_files',
    bash_command="""
    echo "Cleaning up temporary files..."
    
    # Clean up Spark temporary directories
    docker exec spark-submit find /tmp -name "spark-*" -type d -mtime +1 -exec rm -rf {} + || true
    
    # Clean up old backup tables (older than 7 days)
    docker exec spark-submit spark-sql \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,software.amazon.awssdk:bundle:2.20.18,org.apache.hadoop:hadoop-aws:3.3.4 \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.local.type=hadoop \
        --conf spark.sql.catalog.local.warehouse=s3a://iceberg-warehouse/ \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        -e "SHOW TABLES IN local.bronze" | grep backup | head -10 | while read table; do
            echo "Considering cleanup of backup table: $table"
            # Add logic to drop old backup tables based on timestamp
        done
    
    echo "Cleanup completed"
    """,
    trigger_rule='none_failed_or_skipped',
    dag=dag
)

# Define task dependencies
# Sequential pipeline with proper error handling
validate_infra >> check_csv_presence >> initial_quality_check >> bronze_layer >> silver_layer >> gold_layer >> generate_pipeline_report >> [send_success_notification, cleanup_temp_files]

# Error handling flow
bronze_layer >> send_failure_notification
silver_layer >> send_failure_notification
gold_layer >> send_failure_notification 