from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

# Set up logging
logger = logging.getLogger(__name__)

# DAG definition
with DAG(
    dag_id="create_iceberg_tables_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  
    description="Create all Iceberg tables (Silver, Gold, ML) in MinIO via Spark",
    catchup=False,
    tags=["iceberg", "silver", "gold", "tables", "setup"],
    max_active_runs=1
) as dag:

    # Task to check if MinIO is accessible
    check_minio = BashOperator(
        task_id="check_minio_connection",
        bash_command="""
        echo "Checking MinIO connection..."
        curl -s http://minio:9000/minio/health/live || exit 1
        echo "MinIO is accessible"
        """,
        dag=dag
    )

    # Task to check if Spark is ready
    check_spark = BashOperator(
        task_id="check_spark_connection",
        bash_command="""
        echo "Checking Spark connection..."
        curl -s http://spark-master:8080 || exit 1
        echo "Spark is accessible"
        """,
        dag=dag
    )

    # Task to create all tables (Silver, Gold, ML Features)
    create_all_tables = SparkSubmitOperator(
        task_id="create_all_iceberg_tables",
        application="/opt/spark/jobs/ETL/create_iceberg_tables.py",
        conn_id="spark_default",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.executor.memory": "2g",
            "spark.driver.memory": "2g",
            "spark.executor.cores": "2",
            "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
            "spark.sql.catalog.spark_catalog.type": "hive",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.sql.warehouse.dir": "s3a://warehouse",
            "spark.sql.catalog.spark_catalog.warehouse": "s3a://warehouse",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        },
        dag=dag
    )

    # Task to show table information
    show_table_info = PythonOperator(
        task_id="show_table_creation_summary",
        python_callable=lambda: logger.info("""
        🎉 All Iceberg Tables Created Successfully!
        
        📊 SILVER LAYER TABLES:
        - silver_sales: Point of sale transactions with data quality scores
        - silver_inventory: Inventory updates with waste ratios and supply metrics
        - silver_customer_feedback: Customer reviews with sentiment analysis
        - silver_equipment_metrics: Equipment performance and maintenance data
        
        📊 GOLD LAYER DIMENSION TABLES:
        - dim_equipment: Equipment master data
        - dim_store: Store information with SCD Type 2
        - dim_product: Product catalog
        - dim_product_pricing: Product pricing with SCD Type 2
        - dim_customer: Customer information
        - dim_calendar: Date dimension table
        - dim_weather: Weather conditions
        - dim_marketing_events: Marketing campaigns and events
        
        📊 GOLD LAYER FACT TABLES:
        - fact_sales: Sales transactions fact table
        - fact_inventory: Inventory movements fact table
        - fact_promotions: Active promotions
        - fact_customer_feedback: Customer feedback fact table
        - fact_equipment_performance: Equipment performance metrics
        
        🤖 ML FEATURE TABLES:
        - fact_demand_forecast_features: Features for demand forecasting
        - fact_equipment_maintenance_features: Features for predictive maintenance
        - fact_product_quality_features: Features for quality prediction
        
        📁 Locations:
        - Silver: s3a://silver/
        - Gold: s3a://gold/
        - Warehouse: s3a://warehouse/
        
        🔧 Format: Apache Iceberg
        ✅ Ready for: ETL processing and analytics
        """),
        dag=dag
    )

    # Set task dependencies
    check_minio >> check_spark >> create_all_tables >> show_table_info