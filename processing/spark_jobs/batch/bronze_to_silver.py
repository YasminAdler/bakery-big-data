from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
from datetime import datetime, timedelta

def create_spark_session():
    """Create Spark session with Iceberg configuration"""
    return SparkSession.builder \
        .appName("BronzeToSilverETL") \
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

def process_sales_data(spark, process_date):
    """Process sales data from bronze to silver layer"""
    
    # Read from bronze layer
    bronze_df = spark.read \
        .format("iceberg") \
        .load("local.db.bronze_sales_events") \
        .filter(to_date(col("ingestion_time")) == process_date)
    
    # Data quality checks
    bronze_df = bronze_df.filter(
        (col("quantity") > 0) & 
        (col("unit_price") > 0) &
        col("product_id").isNotNull() &
        col("store_id").isNotNull()
    )
    
    # If there's raw_payload, parse it for additional fields
    if "raw_payload" in bronze_df.columns:
        bronze_df = bronze_df.withColumn(
            "payload", from_json(col("raw_payload"), MapType(StringType(), StringType()))
        )
        
        # Extract time_of_day from payload if available
        bronze_df = bronze_df.withColumn(
            "time_of_day", 
            coalesce(col("payload.time_of_day"), 
                    when(hour(col("event_time")).between(5, 11), "MORNING")
                    .when(hour(col("event_time")).between(12, 16), "AFTERNOON")
                    .when(hour(col("event_time")).between(17, 21), "EVENING")
                    .otherwise("NIGHT"))
        )
    else:
        # Create time_of_day from event_time if payload not available
        bronze_df = bronze_df.withColumn(
            "time_of_day", 
            when(hour(col("event_time")).between(5, 11), "MORNING")
            .when(hour(col("event_time")).between(12, 16), "AFTERNOON")
            .when(hour(col("event_time")).between(17, 21), "EVENING")
            .otherwise("NIGHT")
        )
    
    # Transform to silver layer - match silver_sales schema
    silver_df = bronze_df.select(
        col("event_id").alias("sale_id"),
        col("product_id"),
        col("store_id"),
        col("customer_id"),
        to_date(col("event_time")).alias("sale_date"),
        date_format(col("event_time"), "HH:mm:ss").cast("time").alias("sale_time"),
        col("time_of_day"),
        col("quantity").alias("quantity_sold"),
        col("unit_price"),
        (col("quantity") * col("unit_price")).alias("total_revenue"),
        lit(null).cast(StringType()).alias("promo_id"),
        lit(null).cast(StringType()).alias("weather_id"),
        lit(100).cast(IntegerType()).alias("data_quality_score"),  # Default score
        lit("POS").alias("source_system"),
        current_timestamp().alias("etl_timestamp")
    )
    
    # Write to silver layer
    silver_df.write \
        .format("iceberg") \
        .mode("append") \
        .save("local.db.silver_sales")
    
    return silver_df.count()

def process_inventory_data(spark, process_date):
    """Process inventory data from bronze to silver layer"""
    
    # Read from bronze layer
    bronze_df = spark.read \
        .format("iceberg") \
        .load("local.db.bronze_inventory_updates") \
        .filter(to_date(col("ingestion_time")) == process_date)
    
    # Calculate derived fields
    bronze_df = bronze_df.withColumn(
        "closing_stock", 
        col("beginning_stock") + col("restocked_quantity") - col("sold_quantity") - col("waste_quantity")
    ).withColumn(
        "waste_ratio",
        when((col("beginning_stock") + col("restocked_quantity")) > 0,
             col("waste_quantity") / (col("beginning_stock") + col("restocked_quantity"))
        ).otherwise(0.0)
    )
    
    # Calculate days_of_supply based on average daily sales
    avg_daily_sales = bronze_df.groupBy("product_id", "store_id") \
        .agg(avg("sold_quantity").alias("avg_daily_sales"))
    
    bronze_df = bronze_df.join(
        avg_daily_sales,
        ["product_id", "store_id"],
        "left"
    ).withColumn(
        "days_of_supply",
        when(col("avg_daily_sales") > 0,
             col("closing_stock") / col("avg_daily_sales")
        ).otherwise(999)
    )
    
    # Transform to silver layer - match silver_inventory schema
    silver_df = bronze_df.select(
        col("update_id").alias("inventory_id"),
        col("product_id"),
        col("store_id"),
        to_date(col("event_time")).alias("inventory_date"),
        col("beginning_stock"),
        col("restocked_quantity"),
        col("sold_quantity"),
        col("waste_quantity"),
        col("waste_ratio").cast(DecimalType(5,4)),
        col("closing_stock"),
        col("days_of_supply").cast(DecimalType(5,2)),
        lit(100).cast(IntegerType()).alias("data_quality_score"),  # Default score
        lit("INVENTORY").alias("source_system"),
        current_timestamp().alias("etl_timestamp")
    )
    
    # Write to silver layer
    silver_df.write \
        .format("iceberg") \
        .mode("append") \
        .save("local.db.silver_inventory")
    
    return silver_df.count()

def process_feedback_data(spark, process_date):
    """Process customer feedback from bronze to silver layer"""
    
    # Read from bronze layer
    bronze_df = spark.read \
        .format("iceberg") \
        .load("local.db.bronze_customer_feedback") \
        .filter(to_date(col("ingestion_time")) == process_date)
    
    # Parse raw_payload for sentiment analysis
    bronze_df = bronze_df.withColumn(
        "payload", from_json(col("raw_payload"), MapType(StringType(), StringType()))
    )
    
    # Determine sentiment based on rating if not available in payload
    bronze_df = bronze_df.withColumn(
        "sentiment_category",
        coalesce(
            col("payload.sentiment"),
            when(col("rating") >= 4, "POSITIVE")
            .when(col("rating") == 3, "NEUTRAL")
            .otherwise("NEGATIVE")
        )
    )
    
    # Transform to silver layer - match silver_customer_feedback schema
    silver_df = bronze_df.select(
        col("feedback_id"),
        col("customer_id"),
        col("product_id"),
        to_date(col("feedback_time")).alias("feedback_date"),
        col("platform"),
        col("rating"),
        col("review_text"),
        col("sentiment_category"),
        lit("FEEDBACK").alias("source_system"),
        current_timestamp().alias("etl_timestamp")
    )
    
    # Write to silver layer
    silver_df.write \
        .format("iceberg") \
        .mode("append") \
        .save("local.db.silver_customer_feedback")
    
    return silver_df.count()

def process_equipment_data(spark, process_date):
    """Process equipment metrics from bronze to silver layer"""
    
    # Read from bronze layer
    bronze_df = spark.read \
        .format("iceberg") \
        .load("local.db.bronze_equipment_metrics") \
        .filter(to_date(col("ingestion_time")) == process_date)
    
    # Parse raw_payload for temperature and other readings
    bronze_df = bronze_df.withColumn(
        "payload", from_json(col("raw_payload"), MapType(StringType(), StringType()))
    )
    
    # Calculate derived fields
    bronze_df = bronze_df.withColumn(
        "temperature", 
        coalesce(col("payload.temperature").cast(DoubleType()), lit(0.0))
    ).withColumn(
        "operational_hours",
        when(col("operational_status") == "RUNNING", 0.17).otherwise(0.0)  # 10 minutes = 0.17 hours
    ).withColumn(
        "maintenance_alert",
        (col("temperature") > 300) | 
        (col("power_consumption") > 100) |
        (col("operational_status") == "ERROR")
    )
    
    # Transform to silver layer - match silver_equipment_metrics schema
    silver_df = bronze_df.select(
        col("metric_id"),
        col("equipment_id"),
        to_date(col("event_time")).alias("metric_date"),
        date_format(col("event_time"), "HH:mm:ss").cast("time").alias("metric_time"),
        col("power_consumption"),
        col("operational_status"),
        col("operational_hours").cast(DecimalType(5,2)),
        col("maintenance_alert"),
        lit("IOT").alias("source_system"),
        current_timestamp().alias("etl_timestamp")
    )
    
    # Write to silver layer
    silver_df.write \
        .format("iceberg") \
        .mode("append") \
        .save("local.db.silver_equipment_metrics")
    
    return silver_df.count()

def main():
    if len(sys.argv) != 3:
        print("Usage: bronze_to_silver.py <data_type> <process_date>")
        sys.exit(1)
    
    data_type = sys.argv[1]
    process_date = sys.argv[2]
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        if data_type == "sales":
            count = process_sales_data(spark, process_date)
            print(f"Processed {count} sales records for {process_date}")
        elif data_type == "inventory":
            count = process_inventory_data(spark, process_date)
            print(f"Processed {count} inventory records for {process_date}")
        elif data_type == "feedback":
            count = process_feedback_data(spark, process_date)
            print(f"Processed {count} feedback records for {process_date}")
        elif data_type == "equipment":
            count = process_equipment_data(spark, process_date)
            print(f"Processed {count} equipment records for {process_date}")
        elif data_type == "all":
            sales_count = process_sales_data(spark, process_date)
            inventory_count = process_inventory_data(spark, process_date)
            feedback_count = process_feedback_data(spark, process_date)
            equipment_count = process_equipment_data(spark, process_date)
            print(f"Processed: {sales_count} sales, {inventory_count} inventory, "
                  f"{feedback_count} feedback, {equipment_count} equipment records for {process_date}")
        else:
            print(f"Unknown data type: {data_type}")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error processing {data_type} data: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()