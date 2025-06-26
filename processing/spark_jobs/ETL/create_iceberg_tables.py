from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys

def create_spark_session():
    """Create Spark session with Iceberg configuration"""
    return SparkSession.builder \
        .appName("CreateIcebergTables") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://bakery-warehouse/iceberg") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

def create_database(spark):
    """Create database if it doesn't exist"""
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS local.db")
        print("Database 'local.db' created successfully")
    except Exception as e:
        print(f"Error creating database: {str(e)}")

def create_bronze_tables(spark):
    """Create bronze layer tables"""
    
    # Bronze Sales Events Table
    try:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.db.bronze_sales_events (
                event_id STRING,
                product_id STRING,
                store_id STRING,
                customer_id STRING,
                event_time TIMESTAMP,
                quantity INT,
                unit_price DECIMAL(10,2),
                raw_payload STRING,
                ingestion_time TIMESTAMP,
                source_system STRING
            ) USING ICEBERG
            PARTITIONED BY (days(ingestion_time))
            TBLPROPERTIES (
                'write.target-file-size-bytes'='134217728',
                'write.delete.mode'='merge-on-read'
            )
        """)
        print("Bronze sales events table created successfully")
    except Exception as e:
        print(f"Error creating bronze sales events table: {str(e)}")
    
    # Bronze Inventory Updates Table
    try:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.db.bronze_inventory_updates (
                update_id STRING,
                product_id STRING,
                store_id STRING,
                event_time TIMESTAMP,
                beginning_stock INT,
                restocked_quantity INT,
                sold_quantity INT,
                waste_quantity INT,
                raw_payload STRING,
                ingestion_time TIMESTAMP,
                source_system STRING
            ) USING ICEBERG
            PARTITIONED BY (days(ingestion_time))
            TBLPROPERTIES (
                'write.target-file-size-bytes'='134217728',
                'write.delete.mode'='merge-on-read'
            )
        """)
        print("Bronze inventory updates table created successfully")
    except Exception as e:
        print(f"Error creating bronze inventory updates table: {str(e)}")
    
    # Bronze Customer Feedback Table
    try:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.db.bronze_customer_feedback (
                feedback_id STRING,
                customer_id STRING,
                product_id STRING,
                feedback_time TIMESTAMP,
                platform STRING,
                rating INT,
                review_text STRING,
                raw_payload STRING,
                ingestion_time TIMESTAMP,
                source_system STRING
            ) USING ICEBERG
            PARTITIONED BY (days(ingestion_time))
            TBLPROPERTIES (
                'write.target-file-size-bytes'='134217728',
                'write.delete.mode'='merge-on-read'
            )
        """)
        print("Bronze customer feedback table created successfully")
    except Exception as e:
        print(f"Error creating bronze customer feedback table: {str(e)}")
    
    # Bronze Equipment Metrics Table
    try:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.db.bronze_equipment_metrics (
                metric_id STRING,
                equipment_id STRING,
                event_time TIMESTAMP,
                power_consumption DOUBLE,
                operational_status STRING,
                raw_payload STRING,
                ingestion_time TIMESTAMP,
                source_system STRING
            ) USING ICEBERG
            PARTITIONED BY (days(ingestion_time))
            TBLPROPERTIES (
                'write.target-file-size-bytes'='134217728',
                'write.delete.mode'='merge-on-read'
            )
        """)
        print("Bronze equipment metrics table created successfully")
    except Exception as e:
        print(f"Error creating bronze equipment metrics table: {str(e)}")

def create_silver_tables(spark):
    """Create silver layer tables"""
    
    # Silver Sales Table
    try:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.db.silver_sales (
                sale_id STRING,
                product_id STRING,
                store_id STRING,
                customer_id STRING,
                sale_date DATE,
                sale_time TIME,
                time_of_day STRING,
                quantity_sold INT,
                unit_price DECIMAL(10,2),
                total_revenue DECIMAL(10,2),
                promo_id STRING,
                weather_id STRING,
                data_quality_score INT,
                source_system STRING,
                etl_timestamp TIMESTAMP
            ) USING ICEBERG
            PARTITIONED BY (days(sale_date))
            TBLPROPERTIES (
                'write.target-file-size-bytes'='134217728',
                'write.delete.mode'='merge-on-read'
            )
        """)
        print("Silver sales table created successfully")
    except Exception as e:
        print(f"Error creating silver sales table: {str(e)}")
    
    # Silver Inventory Table
    try:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.db.silver_inventory (
                inventory_id STRING,
                product_id STRING,
                store_id STRING,
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
            ) USING ICEBERG
            PARTITIONED BY (days(inventory_date))
            TBLPROPERTIES (
                'write.target-file-size-bytes'='134217728',
                'write.delete.mode'='merge-on-read'
            )
        """)
        print("Silver inventory table created successfully")
    except Exception as e:
        print(f"Error creating silver inventory table: {str(e)}")
    
    # Silver Customer Feedback Table
    try:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.db.silver_customer_feedback (
                feedback_id STRING,
                customer_id STRING,
                product_id STRING,
                feedback_date DATE,
                platform STRING,
                rating INT,
                review_text STRING,
                sentiment_category STRING,
                source_system STRING,
                etl_timestamp TIMESTAMP
            ) USING ICEBERG
            PARTITIONED BY (days(feedback_date))
            TBLPROPERTIES (
                'write.target-file-size-bytes'='134217728',
                'write.delete.mode'='merge-on-read'
            )
        """)
        print("Silver customer feedback table created successfully")
    except Exception as e:
        print(f"Error creating silver customer feedback table: {str(e)}")
    
    # Silver Equipment Metrics Table
    try:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.db.silver_equipment_metrics (
                metric_id STRING,
                equipment_id STRING,
                metric_date DATE,
                metric_time TIME,
                power_consumption DOUBLE,
                operational_status STRING,
                operational_hours DECIMAL(5,2),
                maintenance_alert BOOLEAN,
                source_system STRING,
                etl_timestamp TIMESTAMP
            ) USING ICEBERG
            PARTITIONED BY (days(metric_date))
            TBLPROPERTIES (
                'write.target-file-size-bytes'='134217728',
                'write.delete.mode'='merge-on-read'
            )
        """)
        print("Silver equipment metrics table created successfully")
    except Exception as e:
        print(f"Error creating silver equipment metrics table: {str(e)}")

def create_gold_tables(spark):
    """Create gold layer tables"""
    
    # Gold Daily Sales Summary
    try:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.db.gold_daily_sales_summary (
                summary_id STRING,
                store_id STRING,
                product_id STRING,
                sales_date DATE,
                total_quantity_sold INT,
                total_revenue DECIMAL(12,2),
                average_unit_price DECIMAL(10,2),
                number_of_transactions INT,
                peak_hour STRING,
                seasonal_factor DECIMAL(5,4),
                etl_timestamp TIMESTAMP
            ) USING ICEBERG
            PARTITIONED BY (days(sales_date))
            TBLPROPERTIES (
                'write.target-file-size-bytes'='134217728',
                'write.delete.mode'='merge-on-read'
            )
        """)
        print("Gold daily sales summary table created successfully")
    except Exception as e:
        print(f"Error creating gold daily sales summary table: {str(e)}")
    
    # Gold Customer Analytics
    try:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.db.gold_customer_analytics (
                customer_id STRING,
                analysis_date DATE,
                total_spent DECIMAL(12,2),
                total_orders INT,
                average_order_value DECIMAL(10,2),
                favorite_product_category STRING,
                last_purchase_date DATE,
                customer_lifetime_value DECIMAL(12,2),
                churn_risk_score DECIMAL(3,2),
                sentiment_score DECIMAL(3,2),
                etl_timestamp TIMESTAMP
            ) USING ICEBERG
            PARTITIONED BY (days(analysis_date))
            TBLPROPERTIES (
                'write.target-file-size-bytes'='134217728',
                'write.delete.mode'='merge-on-read'
            )
        """)
        print("Gold customer analytics table created successfully")
    except Exception as e:
        print(f"Error creating gold customer analytics table: {str(e)}")
    
    # Gold Inventory Optimization
    try:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.db.gold_inventory_optimization (
                optimization_id STRING,
                store_id STRING,
                product_id STRING,
                analysis_date DATE,
                current_stock INT,
                recommended_stock INT,
                predicted_demand DECIMAL(8,2),
                waste_trend STRING,
                reorder_point INT,
                economic_order_quantity INT,
                optimization_score DECIMAL(3,2),
                etl_timestamp TIMESTAMP
            ) USING ICEBERG
            PARTITIONED BY (days(analysis_date))
            TBLPROPERTIES (
                'write.target-file-size-bytes'='134217728',
                'write.delete.mode'='merge-on-read'
            )
        """)
        print("Gold inventory optimization table created successfully")
    except Exception as e:
        print(f"Error creating gold inventory optimization table: {str(e)}")
    
    # Gold ML Features Table
    try:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.db.gold_ml_features (
                feature_id STRING,
                entity_type STRING,
                entity_id STRING,
                feature_date DATE,
                feature_vector MAP<STRING, DOUBLE>,
                target_variable DOUBLE,
                model_version STRING,
                etl_timestamp TIMESTAMP
            ) USING ICEBERG
            PARTITIONED BY (days(feature_date))
            TBLPROPERTIES (
                'write.target-file-size-bytes'='134217728',
                'write.delete.mode'='merge-on-read'
            )
        """)
        print("Gold ML features table created successfully")
    except Exception as e:
        print(f"Error creating gold ML features table: {str(e)}")

def verify_tables(spark):
    """Verify all tables were created successfully"""
    print("\n=== Verifying Table Creation ===")
    
    tables = [
        "local.db.bronze_sales_events",
        "local.db.bronze_inventory_updates", 
        "local.db.bronze_customer_feedback",
        "local.db.bronze_equipment_metrics",
        "local.db.silver_sales",
        "local.db.silver_inventory",
        "local.db.silver_customer_feedback",
        "local.db.silver_equipment_metrics",
        "local.db.gold_daily_sales_summary",
        "local.db.gold_customer_analytics",
        "local.db.gold_inventory_optimization",
        "local.db.gold_ml_features"
    ]
    
    for table in tables:
        try:
            spark.sql(f"DESCRIBE {table}").count()
            print(f"✓ {table} exists and accessible")
        except Exception as e:
            print(f"✗ {table} failed: {str(e)}")

def main():
    layer = sys.argv[1] if len(sys.argv) > 1 else "all"
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        print("Creating Iceberg database...")
        create_database(spark)
        
        if layer in ["all", "bronze"]:
            print("\nCreating Bronze layer tables...")
            create_bronze_tables(spark)
        
        if layer in ["all", "silver"]:
            print("\nCreating Silver layer tables...")
            create_silver_tables(spark)
        
        if layer in ["all", "gold"]:
            print("\nCreating Gold layer tables...")
            create_gold_tables(spark)
        
        if layer == "all":
            verify_tables(spark)
        
        print(f"\nIceberg table creation completed for layer: {layer}")
        
    except Exception as e:
        print(f"Error in table creation: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()