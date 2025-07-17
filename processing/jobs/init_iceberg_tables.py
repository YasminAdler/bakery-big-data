#!/usr/bin/env python3
"""
Initialize Iceberg Tables for Bakery Data Pipeline
Creates all tables in Bronze, Silver, and Gold layers
"""

from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create Spark session with Iceberg configuration"""
    return SparkSession.builder \
        .appName("Initialize Iceberg Tables") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://iceberg-warehouse/") \
        .getOrCreate()


def create_bronze_tables(spark):
    """Create Bronze layer tables"""
    logger.info("Creating Bronze layer tables...")
    
    # Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS local.bronze")
    
    # Bronze Sales Events
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.sales_events (
            event_id STRING,
            event_time TIMESTAMP,
            ingestion_time TIMESTAMP,
            product_id INT,
            store_id INT,
            quantity INT,
            unit_price DECIMAL(10,2),
            customer_id STRING,
            date DATE,
            time_of_day STRING,
            processing_status STRING
        ) USING iceberg
        PARTITIONED BY (date)
    """)
    
    # Bronze Inventory Updates
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.inventory_updates (
            update_id STRING,
            event_time TIMESTAMP,
            ingestion_time TIMESTAMP,
            product_id INT,
            store_id INT,
            beginning_stock INT,
            restocked_quantity INT,
            sold_quantity INT,
            waste_quantity INT,
            reported_by STRING,
            processing_status STRING,
            late_arrival_hours DOUBLE
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """)
    
    # Bronze Customer Feedback
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.customer_feedback (
            feedback_id STRING,
            feedback_time TIMESTAMP,
            ingestion_time TIMESTAMP,
            customer_id STRING,
            product_id INT,
            rating INT,
            platform STRING,
            review_text STRING,
            raw_payload STRING,
            processing_status STRING
        ) USING iceberg
        PARTITIONED BY (days(feedback_time))
    """)
    
    # Bronze Equipment Metrics
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.equipment_metrics (
            metric_id STRING,
            equipment_id INT,
            event_time TIMESTAMP,
            ingestion_time TIMESTAMP,
            power_consumption DECIMAL(8,2),
            operational_status STRING,
            raw_payload STRING,
            processing_status STRING
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """)
    
    # Bronze Promotions (Batch)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.promotions (
            promo_id STRING,
            product_id INT,
            promo_type STRING,
            start_date DATE,
            end_date DATE,
            discount_percentage DECIMAL(5,2),
            raw_payload STRING,
            processing_status STRING
        ) USING iceberg
    """)
    
    # Bronze Weather Data (Batch)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.weather_data (
            weather_id STRING,
            date DATE,
            store_id INT,
            weather_condition STRING,
            raw_payload STRING,
            processing_status STRING
        ) USING iceberg
        PARTITIONED BY (date)
    """)
    
    logger.info("Bronze layer tables created successfully")


def create_silver_tables(spark):
    """Create Silver layer tables"""
    logger.info("Creating Silver layer tables...")
    
    # Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS local.silver")
    
    # Silver Sales
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.silver.sales (
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
    
    # Silver Inventory
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.silver.inventory (
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
    
    # Silver Customer Feedback
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.silver.customer_feedback (
            feedback_id STRING,
            customer_id STRING,
            product_id INT,
            feedback_date DATE,
            platform STRING,
            rating INT,
            review_text STRING,
            sentiment_category STRING,
            data_quality_score INT,
            source_system STRING,
            etl_timestamp TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (feedback_date)
    """)
    
    # Silver Equipment Metrics
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.silver.equipment_metrics (
            metric_id STRING,
            equipment_id INT,
            metric_date DATE,
            metric_time STRING,
            power_consumption DECIMAL(8,2),
            operational_status STRING,
            operational_hours DECIMAL(5,2),
            maintenance_alert BOOLEAN,
            data_quality_score INT,
            source_system STRING,
            etl_timestamp TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (metric_date)
    """)
    
    logger.info("Silver layer tables created successfully")


def create_gold_dimension_tables(spark):
    """Create Gold layer dimension tables"""
    logger.info("Creating Gold layer dimension tables...")
    
    # Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS local.gold")
    
    # Dim Equipment
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
    
    # Dim Store (SCD Type 2)
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
    
    # Dim Product
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
    
    # Dim Product Pricing (SCD Type 2)
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
    
    # Dim Customer
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
    
    # Dim Calendar
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
    
    # Dim Weather
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
    
    # Dim Marketing Events
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
    
    logger.info("Gold layer dimension tables created successfully")


def create_gold_fact_tables(spark):
    """Create Gold layer fact tables"""
    logger.info("Creating Gold layer fact tables...")
    
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
    
    logger.info("Gold layer fact tables created successfully")


def create_ml_feature_tables(spark):
    """Create ML feature tables"""
    logger.info("Creating ML feature tables...")
    
    # Demand Forecast Features
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
    
    # Equipment Maintenance Features
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
    
    # Product Quality Features
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
    
    logger.info("ML feature tables created successfully")


def main():
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        # Create all tables
        create_bronze_tables(spark)
        create_silver_tables(spark)
        create_gold_dimension_tables(spark)
        create_gold_fact_tables(spark)
        create_ml_feature_tables(spark)
        
        # Show all created tables
        logger.info("\n=== Created Tables ===")
        spark.sql("SHOW DATABASES IN local").show()
        
        for db in ['bronze', 'silver', 'gold']:
            logger.info(f"\nTables in {db} database:")
            spark.sql(f"SHOW TABLES IN local.{db}").show()
        
        logger.info("All Iceberg tables initialized successfully!")
        
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main() 