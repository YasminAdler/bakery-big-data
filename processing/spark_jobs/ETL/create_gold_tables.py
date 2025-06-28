"""
Create Gold Layer Tables
Standalone script to create all gold layer Iceberg tables.
"""

from pyspark.sql import SparkSession

def create_gold_tables():
    """Create all gold layer Iceberg tables"""
    
    # Initialize Spark session with Iceberg support
    spark = SparkSession.builder \
        .appName("GoldTablesCreation") \
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
    
    # Create database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS local.gold")
    
    # Create Dimension Tables
    
    # Equipment Dimension
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.dim_equipment (
            equipment_id INT,
            equipment_name STRING,
            equipment_type STRING,
            manufacturer STRING,
            model_number STRING,
            purchase_date DATE,
            power_consumption_kw DECIMAL(8,2),
            maintenance_frequency STRING,
            expected_lifespan_years INT,
            replacement_cost DECIMAL(10,2),
            is_current BOOLEAN
        ) USING iceberg
    """)
    
    # Store Dimension (SCD type 2)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.dim_store (
            store_key INT,
            store_id INT,           
            location STRING,
            type STRING,
            address STRING,
            city STRING,
            region STRING,
            country STRING,
            manager_name STRING,
            opening_hour DECIMAL(5,2),         
            closing_hour DECIMAL(5,2),            
            total_area_sqm DECIMAL(10,2),
            effective_date DATE,       
            end_date DATE,           
            is_current_flag BOOLEAN,
            area_sqm DECIMAL(10,2)
        ) USING iceberg
    """)
    
    # Product Dimension
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.dim_product (
            product_id INT,
            product_name STRING,
            category STRING,
            subcategory STRING,
            ingredients STRING,
            allergens STRING,
            shelf_life_hours INT,
            is_seasonal BOOLEAN,
            nutrition_info STRING
        ) USING iceberg
    """)
    
    # Product Pricing Dimension (SCD Type 2)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.dim_product_pricing (
            pricing_key INT,
            product_id INT,
            product_name STRING,
            base_price DECIMAL(10,2),
            price_category STRING,
            marketing_strategy STRING,
            base_price_creation_date DATE,
            effective_date DATE, 
            end_date DATE,
            is_current_record BOOLEAN
        ) USING iceberg
    """)
    
    # Customer Dimension
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.dim_customer (
            customer_key INT,
            customer_id STRING,
            customer_name STRING,
            email STRING,
            phone STRING,
            address STRING,
            city STRING,
            region STRING,
            country STRING,
            joining_date DATE,               
            last_visit_date DATE,
            is_current_record BOOLEAN
        ) USING iceberg
    """)
    
    # Calendar Dimension
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.dim_calendar (
            date DATE,
            day_of_week STRING,
            day_of_month INT,
            month INT,
            month_name STRING,
            quarter INT,
            year INT,
            is_weekend BOOLEAN,
            is_holiday BOOLEAN,
            holiday_name STRING,
            season STRING
        ) USING iceberg
    """)
    
    # Weather Dimension
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.dim_weather (
            weather_id STRING,
            date DATE,
            store_id INT,
            weather_condition STRING,
            humidity DECIMAL(5,2),
            wind_speed DECIMAL(5,2)
        ) USING iceberg
        PARTITIONED BY (date)
    """)
    
    # Marketing Events Dimension
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.dim_marketing_events (
            event_id STRING,
            event_name STRING,
            start_date DATE,
            end_date DATE,
            affected_products STRING,
            discount_percentage DECIMAL(5,2),
            campaign_budget DECIMAL(10,2),
            target_audience STRING,
            channel STRING
        ) USING iceberg
    """)
    
    # Create Fact Tables
    
    # Fact Sales
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.fact_sales (
            sale_id STRING,
            product_id INT,
            store_id INT,
            customer_key INT,
            pricing_key INT,
            date DATE,
            weather_id STRING,
            marketing_event_id STRING,
            quantity_sold INT,
            total_revenue DECIMAL(10,2),
            customer_loyalty_tier STRING,
            time_of_day STRING,
            promotion_applied BOOLEAN,
            discount_amount DECIMAL(10,2),
            profit_margin DECIMAL(5,2),
            cost_of_goods_sold DECIMAL(10,2),
            transaction_sequence INT
        ) USING iceberg
        PARTITIONED BY (date)
    """)
    
    # Fact Inventory
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.fact_inventory (
            inventory_id STRING,
            product_id INT,
            store_id INT,
            date DATE,
            beginning_stock INT,
            restocked_quantity INT,
            sold_quantity INT,
            waste_quantity INT,
            waste_ratio DECIMAL(5,4),
            days_of_supply DECIMAL(5,2),
            stock_out_events INT
        ) USING iceberg
        PARTITIONED BY (date)
    """)
    
    # Fact Promotions
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.fact_promotions (
            promo_id STRING,
            product_id INT,
            promo_type STRING,
            start_date DATE,
            end_date DATE,
            is_active BOOLEAN,
            discount_percentage DECIMAL(5,2),
            target_audience STRING,
            sales_lift_percentage DECIMAL(5,2),
            description STRING
        ) USING iceberg
    """)
    
    # Fact Customer Feedback
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.fact_customer_feedback (
            feedback_id STRING,
            customer_key INT,
            product_id INT,
            platform STRING,
            rating INT,
            review_text STRING,
            feedback_date DATE
        ) USING iceberg
        PARTITIONED BY (feedback_date)
    """)
    
    # Fact Equipment Performance
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.fact_equipment_performance (
            performance_id STRING,
            equipment_id INT,
            date DATE,
            operational_hours DECIMAL(5,2),
            avg_temperature DECIMAL(5,2),
            total_power_consumption DECIMAL(8,2),
            maintenance_events INT,
            downtime_minutes INT,
            efficiency_score DECIMAL(5,2)
        ) USING iceberg
        PARTITIONED BY (date)
    """)
    
    # ML Feature Tables
    
    # Demand Forecasting Features
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.fact_demand_forecast_features (
            feature_id STRING,
            product_id INT,
            store_id INT,
            date DATE,
            sales_volume INT,
            avg_daily_sales DECIMAL(10,2),
            sales_last_7_days DECIMAL(10,2),
            sales_last_30_days DECIMAL(10,2),
            sales_trend STRING,
            seasonality_index DECIMAL(5,4),
            day_of_week STRING,
            is_holiday BOOLEAN,
            holiday_name STRING,
            season STRING,
            promotion_active BOOLEAN,
            marketing_campaign STRING,
            inventory_level INT,
            waste_ratio DECIMAL(5,4),
            price DECIMAL(10,2),
            weather_impact_score DECIMAL(5,2),
            forecast_accuracy DECIMAL(5,4)
        ) USING iceberg
        PARTITIONED BY (date)
    """)
    
    # Equipment Maintenance Prediction Features
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.fact_equipment_maintenance_features (
            feature_id STRING,
            equipment_id INT,
            date DATE,
            days_since_last_maintenance INT,
            total_operational_hours DECIMAL(10,2),
            avg_daily_temperature DECIMAL(5,2),
            temperature_fluctuation DECIMAL(5,2),
            power_consumption_trend DECIMAL(8,2),
            abnormal_events_count INT,
            age_in_days INT,
            usage_intensity_score DECIMAL(5,2),
            maintenance_history_score DECIMAL(5,2),
            failure_probability DECIMAL(5,4)
        ) USING iceberg
        PARTITIONED BY (date)
    """)
    
    # Product Quality Prediction Features
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.fact_product_quality_features (
            feature_id STRING,
            product_id INT,
            production_batch_id STRING,
            production_date DATE,
            equipment_id INT,
            baker_id INT,
            ingredient_quality_score DECIMAL(5,2),
            process_adherence_score DECIMAL(5,2),
            proofing_time_minutes INT,
            baking_temperature DECIMAL(5,2),
            baking_time_minutes INT,
            cooling_time_minutes INT,
            ambient_humidity DECIMAL(5,2),
            freshness_score DECIMAL(5,2),
            waste_probability DECIMAL(5,4)
        ) USING iceberg
        PARTITIONED BY (production_date)
    """)
    
    print("✅ All Gold layer tables created successfully!")
    spark.stop()

if __name__ == "__main__":
    create_gold_tables() 