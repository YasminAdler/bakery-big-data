"""
Create Silver Layer Tables
Standalone script to create all silver layer Iceberg tables.
"""

from pyspark.sql import SparkSession

def create_silver_tables():
    """Create the silver layer Iceberg tables"""
    
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

if __name__ == "__main__":
    create_silver_tables() 