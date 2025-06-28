"""
Bronze to Silver ETL Job
Transforms raw data from bronze layer to standardized silver layer with data quality checks.
"""

import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Iceberg support"""
    return SparkSession.builder \
        .appName("BronzeToSilverETL") \
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

def calculate_data_quality_score(df, rules):
    """Calculate data quality score based on defined rules"""
    score = 100
    
    for rule in rules:
        if rule['type'] == 'null_check':
            null_count = df.filter(col(rule['column']).isNull()).count()
            total_count = df.count()
            if total_count > 0:
                null_ratio = null_count / total_count
                score -= (null_ratio * 20)  # Deduct up to 20 points for nulls
        
        elif rule['type'] == 'range_check':
            invalid_count = df.filter(
                (col(rule['column']) < rule['min']) | 
                (col(rule['column']) > rule['max'])
            ).count()
            total_count = df.count()
            if total_count > 0:
                invalid_ratio = invalid_count / total_count
                score -= (invalid_ratio * 15)  # Deduct up to 15 points for range violations
    
    return max(0, int(score))

def transform_pos_data(spark, bronze_table, silver_table):
    """Transform POS data from bronze to silver"""
    logger.info(f"Transforming POS data from {bronze_table} to {silver_table}")
    
    # Read from bronze layer
    bronze_df = spark.read.format("iceberg").load(bronze_table)
    
    # Apply transformations
    silver_df = bronze_df.select(
        col("event_id").alias("sale_id"),
        col("product_id"),
        col("store_id"),
        col("customer_id"),
        to_date(col("event_time")).alias("sale_date"),
        date_format(col("event_time"), "HH:mm:ss").alias("sale_time"),
        when(hour(col("event_time")).between(6, 11), "morning")
        .when(hour(col("event_time")).between(12, 16), "afternoon")
        .when(hour(col("event_time")).between(17, 21), "evening")
        .otherwise("night").alias("time_of_day"),
        col("quantity"),
        col("unit_price"),
        (col("quantity") * col("unit_price")).alias("total_revenue"),
        lit(None).cast("string").alias("promo_id"),  # Will be enriched later
        lit(None).cast("string").alias("weather_id"),  # Will be enriched later
        lit(100).cast("int").alias("data_quality_score"),
        lit("POS_SYSTEM").alias("source_system"),
        current_timestamp().alias("etl_timestamp")
    )
    
    # Data quality checks
    quality_rules = [
        {"type": "null_check", "column": "product_id"},
        {"type": "null_check", "column": "store_id"},
        {"type": "range_check", "column": "quantity", "min": 1, "max": 100},
        {"type": "range_check", "column": "unit_price", "min": 0.01, "max": 1000.00}
    ]
    
    quality_score = calculate_data_quality_score(silver_df, quality_rules)
    
    # Update quality score
    silver_df = silver_df.withColumn("data_quality_score", lit(quality_score))
    
    # Write to silver layer
    silver_df.writeTo(silver_table).overwritePartitions()
    
    logger.info(f"✅ POS data transformed successfully. Quality score: {quality_score}")
    return silver_df.count()

def transform_inventory_data(spark, bronze_table, silver_table):
    """Transform inventory data from bronze to silver"""
    logger.info(f"Transforming inventory data from {bronze_table} to {silver_table}")
    
    # Read from bronze layer
    bronze_df = spark.read.format("iceberg").load(bronze_table)
    
    # Apply transformations
    silver_df = bronze_df.select(
        col("event_id").alias("inventory_id"),
        col("product_id"),
        col("store_id"),
        to_date(col("event_time")).alias("inventory_date"),
        col("beginning_stock"),
        col("restocked_quantity"),
        col("sold_quantity"),
        col("waste_quantity"),
        when(col("beginning_stock") > 0, 
             round(col("waste_quantity") / col("beginning_stock"), 4)
        ).otherwise(0.0).alias("waste_ratio"),
        (col("beginning_stock") + col("restocked_quantity") - 
         col("sold_quantity") - col("waste_quantity")).alias("closing_stock"),
        when(col("sold_quantity") > 0,
             round(col("closing_stock") / (col("sold_quantity") / 1.0), 2)
        ).otherwise(0.0).alias("days_of_supply"),
        lit(100).cast("int").alias("data_quality_score"),
        lit("INVENTORY_SYSTEM").alias("source_system"),
        current_timestamp().alias("etl_timestamp")
    )
    
    # Data quality checks
    quality_rules = [
        {"type": "null_check", "column": "product_id"},
        {"type": "null_check", "column": "store_id"},
        {"type": "range_check", "column": "beginning_stock", "min": 0, "max": 10000},
        {"type": "range_check", "column": "sold_quantity", "min": 0, "max": 1000}
    ]
    
    quality_score = calculate_data_quality_score(silver_df, quality_rules)
    silver_df = silver_df.withColumn("data_quality_score", lit(quality_score))
    
    # Write to silver layer
    silver_df.writeTo(silver_table).overwritePartitions()
    
    logger.info(f"✅ Inventory data transformed successfully. Quality score: {quality_score}")
    return silver_df.count()

def transform_feedback_data(spark, bronze_table, silver_table):
    """Transform customer feedback data from bronze to silver"""
    logger.info(f"Transforming feedback data from {bronze_table} to {silver_table}")
    
    # Read from bronze layer
    bronze_df = spark.read.format("iceberg").load(bronze_table)
    
    # Apply transformations
    silver_df = bronze_df.select(
        col("event_id").alias("feedback_id"),
        col("customer_id"),
        col("product_id"),
        to_date(col("event_time")).alias("feedback_date"),
        col("platform"),
        col("rating"),
        col("review_text"),
        when(col("rating") >= 4, "positive")
        .when(col("rating") == 3, "neutral")
        .otherwise("negative").alias("sentiment_category"),
        lit(100).cast("int").alias("data_quality_score"),
        lit("FEEDBACK_SYSTEM").alias("source_system"),
        current_timestamp().alias("etl_timestamp")
    )
    
    # Data quality checks
    quality_rules = [
        {"type": "null_check", "column": "customer_id"},
        {"type": "null_check", "column": "product_id"},
        {"type": "range_check", "column": "rating", "min": 1, "max": 5}
    ]
    
    quality_score = calculate_data_quality_score(silver_df, quality_rules)
    silver_df = silver_df.withColumn("data_quality_score", lit(quality_score))
    
    # Write to silver layer
    silver_df.writeTo(silver_table).overwritePartitions()
    
    logger.info(f"✅ Feedback data transformed successfully. Quality score: {quality_score}")
    return silver_df.count()

def transform_iot_data(spark, bronze_table, silver_table):
    """Transform IoT equipment data from bronze to silver"""
    logger.info(f"Transforming IoT data from {bronze_table} to {silver_table}")
    
    # Read from bronze layer
    bronze_df = spark.read.format("iceberg").load(bronze_table)
    
    # Apply transformations
    silver_df = bronze_df.select(
        col("event_id").alias("metric_id"),
        col("equipment_id"),
        to_date(col("event_time")).alias("metric_date"),
        date_format(col("event_time"), "HH:mm:ss").alias("metric_time"),
        col("power_consumption"),
        col("operational_status"),
        col("operational_hours"),
        col("maintenance_alert"),
        lit(100).cast("int").alias("data_quality_score"),
        lit("IOT_SYSTEM").alias("source_system"),
        current_timestamp().alias("etl_timestamp")
    )
    
    # Data quality checks
    quality_rules = [
        {"type": "null_check", "column": "equipment_id"},
        {"type": "range_check", "column": "power_consumption", "min": 0, "max": 100},
        {"type": "range_check", "column": "operational_hours", "min": 0, "max": 24}
    ]
    
    quality_score = calculate_data_quality_score(silver_df, quality_rules)
    silver_df = silver_df.withColumn("data_quality_score", lit(quality_score))
    
    # Write to silver layer
    silver_df.writeTo(silver_table).overwritePartitions()
    
    logger.info(f"✅ IoT data transformed successfully. Quality score: {quality_score}")
    return silver_df.count()

def main():
    """Main ETL process"""
    logger.info("🚀 Starting Bronze to Silver ETL process...")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Define table names
        bronze_pos = "local.db.bronze_pos_events"
        bronze_inventory = "local.db.bronze_inventory_events"
        bronze_feedback = "local.db.bronze_feedback_events"
        bronze_iot = "local.db.bronze_iot_events"
        
        silver_sales = "local.db.silver_sales"
        silver_inventory = "local.db.silver_inventory"
        silver_feedback = "local.db.silver_customer_feedback"
        silver_equipment = "local.db.silver_equipment_metrics"
        
        # Transform each data type
        pos_count = transform_pos_data(spark, bronze_pos, silver_sales)
        inventory_count = transform_inventory_data(spark, bronze_inventory, silver_inventory)
        feedback_count = transform_feedback_data(spark, bronze_feedback, silver_feedback)
        iot_count = transform_iot_data(spark, bronze_iot, silver_equipment)
        
        # Log summary
        logger.info("📊 ETL Summary:")
        logger.info(f"   - POS records processed: {pos_count}")
        logger.info(f"   - Inventory records processed: {inventory_count}")
        logger.info(f"   - Feedback records processed: {feedback_count}")
        logger.info(f"   - IoT records processed: {iot_count}")
        logger.info(f"   - Total records: {pos_count + inventory_count + feedback_count + iot_count}")
        
        logger.info("✅ Bronze to Silver ETL completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ ETL process failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()