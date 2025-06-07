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
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://bakery-warehouse/iceberg") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def process_sales_data(spark, process_date):
    """Process sales data from bronze to silver layer"""
    
    # Read from bronze layer
    bronze_df = spark.read \
        .format("iceberg") \
        .load("local.bronze.sales_events") \
        .filter(col("ingestion_time").cast("date") == process_date)
    
    # Data quality checks
    bronze_df = bronze_df.filter(
        (col("quantity") > 0) & 
        (col("unit_price") > 0) &
        col("product_id").isNotNull() &
        col("store_id").isNotNull()
    )
    
    # Transform to silver layer
    silver_df = bronze_df.select(
        col("event_id").alias("sale_id"),
        col("product_id"),
        col("store_id"),
        col("customer_id"),
        to_date(col("event_time")).alias("sale_date"),
        date_format(col("event_time"), "HH:mm:ss").alias("sale_time"),
        col("time_of_day"),
        col("quantity").alias("quantity_sold"),
        col("unit_price"),
        (col("quantity") * col("unit_price")).alias("total_revenue"),
        col("raw_payload.discount_applied").alias("discount_percentage"),
        lit(100).alias("data_quality_score"),  # Simplified for demo
        lit("POS").alias("source_system"),
        current_timestamp().alias("etl_timestamp")
    )
    
    # Calculate additional metrics
    silver_df = silver_df.withColumn(
        "discount_amount",
        col("total_revenue") * col("discount_percentage") / 100
    ).withColumn(
        "cost_of_goods_sold",
        col("total_revenue") * 0.6  # Assuming 60% cost ratio
    ).withColumn(
        "profit_margin",
        (col("total_revenue") - col("cost_of_goods_sold")) / col("total_revenue") * 100
    )
    
    # Write to silver layer
    silver_df.write \
        .format("iceberg") \
        .mode("append") \
        .save("local.silver.sales")
    
    return silver_df.count()

def process_inventory_data(spark, process_date):
    """Process inventory data from bronze to silver layer"""
    
    # Read from bronze layer
    bronze_df = spark.read \
        .format("iceberg") \
        .load("local.bronze.inventory_updates") \
        .filter(col("ingestion_time").cast("date") == process_date)
    
    # Handle late-arriving data
    bronze_df = bronze_df.withColumn(
        "is_late_arrival",
        datediff(col("ingestion_time"), col("event_time")) > 2
    )
    
    # Transform to silver layer
    silver_df = bronze_df.select(
        col("update_id").alias("inventory_id"),
        col("product_id"),
        col("store_id"),
        to_date(col("event_time")).alias("inventory_date"),
        col("beginning_stock"),
        col("restocked_quantity"),
        col("sold_quantity"),
        col("waste_quantity"),
        when(col("sold_quantity") > 0, 
             col("waste_quantity") / col("sold_quantity")
        ).otherwise(0).alias("waste_ratio"),
        (col("beginning_stock") + col("restocked_quantity") - 
         col("sold_quantity") - col("waste_quantity")).alias("closing_stock"),
        lit(100).alias("data_quality_score"),
        lit("INVENTORY").alias("source_system"),
        current_timestamp().alias("etl_timestamp")
    )
    
    # Calculate days of supply
    avg_daily_sales = silver_df.groupBy("product_id", "store_id") \
        .agg(avg("sold_quantity").alias("avg_daily_sales"))
    
    silver_df = silver_df.join(
        avg_daily_sales,
        ["product_id", "store_id"],
        "left"
    ).withColumn(
        "days_of_supply",
        when(col("avg_daily_sales") > 0,
             col("closing_stock") / col("avg_daily_sales")
        ).otherwise(999)
    )
    
    # Add inventory value and status
    silver_df = silver_df.withColumn(
        "inventory_value",
        col("closing_stock") * 2.5  # Assuming average unit cost
    ).withColumn(
        "stock_status",
        when(col("days_of_supply") < 1, "CRITICAL")
        .when(col("days_of_supply") < 3, "LOW")
        .when(col("days_of_supply") > 14, "EXCESS")
        .otherwise("NORMAL")
    )
    
    # Write to silver layer
    silver_df.write \
        .format("iceberg") \
        .mode("append") \
        .save("local.silver.inventory")
    
    return silver_df.count()

def process_equipment_data(spark, process_date):
    """Process equipment metrics from bronze to silver layer"""
    
    # Read from bronze layer
    bronze_df = spark.read \
        .format("iceberg") \
        .load("local.bronze.equipment_metrics") \
        .filter(col("ingestion_time").cast("date") == process_date)
    
    # Transform to silver layer
    silver_df = bronze_df.select(
        col("metric_id"),
        col("equipment_id"),
        to_date(col("event_time")).alias("metric_date"),
        date_format(col("event_time"), "HH:mm:ss").alias("metric_time"),
        col("sensor_readings.temperature").alias("temperature"),
        col("power_consumption"),
        col("operational_status"),
        lit("IOT").alias("source_system"),
        current_timestamp().alias("etl_timestamp")
    )
    
    # Add operational hours and maintenance alerts
    silver_df = silver_df.withColumn(
        "operational_hours",
        when(col("operational_status") == "RUNNING", 1.0).otherwise(0.0)
    ).withColumn(
        "maintenance_alert",
        (col("temperature") > 250) | (col("power_consumption") > 100)
    )
    
    # Write to silver layer
    silver_df.write \
        .format("iceberg") \
        .mode("append") \
        .save("local.silver.equipment_metrics")
    
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
        elif data_type == "equipment":
            count = process_equipment_data(spark, process_date)
            print(f"Processed {count} equipment records for {process_date}")
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