#!/usr/bin/env python3
"""
Stream to Bronze Layer
Consumes data from Kafka topics and writes to Bronze Iceberg tables
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create Spark session with Kafka and Iceberg configuration"""
    return SparkSession.builder \
        .appName("Stream to Bronze Layer") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://iceberg-warehouse/") \
        .getOrCreate()


def process_sales_events_stream(spark):
    """Process sales events from Kafka to Bronze"""
    logger.info("Starting sales events stream processing...")
    
    # Define schema for sales events
    sales_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("ingestion_time", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("customer_id", StringType(), True),
        StructField("date", StringType(), True),
        StructField("time_of_day", StringType(), True),
        StructField("processing_status", StringType(), True)
    ])
    
    # Read from Kafka
    sales_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "sales-events") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON and transform
    sales_bronze = sales_stream \
        .select(from_json(col("value").cast("string"), sales_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("event_time"))) \
        .withColumn("ingestion_time", to_timestamp(col("ingestion_time"))) \
        .withColumn("date", to_date(col("date"))) \
        .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2)))
    
    # Write to Iceberg table
    query = sales_bronze \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .option("path", "local.bronze.sales_events") \
        .option("checkpointLocation", "s3a://iceberg-warehouse/checkpoints/bronze_sales_events") \
        .start()
    
    return query


def process_equipment_metrics_stream(spark):
    """Process equipment metrics from Kafka to Bronze"""
    logger.info("Starting equipment metrics stream processing...")
    
    # Define schema for equipment metrics
    metrics_schema = StructType([
        StructField("metric_id", StringType(), True),
        StructField("equipment_id", IntegerType(), True),
        StructField("event_time", StringType(), True),
        StructField("ingestion_time", StringType(), True),
        StructField("power_consumption", DoubleType(), True),
        StructField("operational_status", StringType(), True),
        StructField("raw_payload", StringType(), True),
        StructField("processing_status", StringType(), True)
    ])
    
    # Read from Kafka
    metrics_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "equipment-metrics") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON and transform
    metrics_bronze = metrics_stream \
        .select(from_json(col("value").cast("string"), metrics_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("event_time"))) \
        .withColumn("ingestion_time", to_timestamp(col("ingestion_time"))) \
        .withColumn("power_consumption", col("power_consumption").cast(DecimalType(8, 2))) \
        .withColumn("raw_payload", to_json(from_json(col("raw_payload"), MapType(StringType(), StringType()))))
    
    # Write to Iceberg table
    query = metrics_bronze \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .option("path", "local.bronze.equipment_metrics") \
        .option("checkpointLocation", "s3a://iceberg-warehouse/checkpoints/bronze_equipment_metrics") \
        .start()
    
    return query


def process_inventory_updates_stream(spark):
    """Process inventory updates from Kafka to Bronze (with late arrival handling)"""
    logger.info("Starting inventory updates stream processing...")
    
    # Define schema for inventory updates
    inventory_schema = StructType([
        StructField("update_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("ingestion_time", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("beginning_stock", IntegerType(), True),
        StructField("restocked_quantity", IntegerType(), True),
        StructField("sold_quantity", IntegerType(), True),
        StructField("waste_quantity", IntegerType(), True),
        StructField("reported_by", StringType(), True),
        StructField("processing_status", StringType(), True),
        StructField("late_arrival_hours", DoubleType(), True)
    ])
    
    # Read from Kafka
    inventory_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "inventory-updates") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON and transform with watermark for late arrivals (48 hours)
    inventory_bronze = inventory_stream \
        .select(from_json(col("value").cast("string"), inventory_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("event_time"))) \
        .withColumn("ingestion_time", to_timestamp(col("ingestion_time"))) \
        .withWatermark("event_time", "48 hours")  # Handle late arrivals up to 48 hours
    
    # Write to Iceberg table
    query = inventory_bronze \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="60 seconds") \
        .option("path", "local.bronze.inventory_updates") \
        .option("checkpointLocation", "s3a://iceberg-warehouse/checkpoints/bronze_inventory_updates") \
        .start()
    
    return query


def monitor_queries(queries):
    """Monitor streaming queries and handle failures"""
    logger.info("Monitoring streaming queries...")
    
    try:
        # Wait for all queries to terminate
        for query in queries:
            query.awaitTermination()
    except Exception as e:
        logger.error(f"Streaming query error: {str(e)}")
        for query in queries:
            if query.isActive:
                query.stop()
        raise


def main():
    """Main execution function"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Start all streaming queries
        queries = []
        
        # Process sales events
        sales_query = process_sales_events_stream(spark)
        queries.append(sales_query)
        logger.info(f"Sales events streaming query started: {sales_query.id}")
        
        # Process equipment metrics
        metrics_query = process_equipment_metrics_stream(spark)
        queries.append(metrics_query)
        logger.info(f"Equipment metrics streaming query started: {metrics_query.id}")
        
        # Process inventory updates
        inventory_query = process_inventory_updates_stream(spark)
        queries.append(inventory_query)
        logger.info(f"Inventory updates streaming query started: {inventory_query.id}")
        
        # Monitor queries
        monitor_queries(queries)
        
    except Exception as e:
        logger.error(f"Error in streaming job: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main() 