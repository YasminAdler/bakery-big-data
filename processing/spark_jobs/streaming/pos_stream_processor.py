from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

def create_spark_session():
    """Create Spark session for streaming"""
    return SparkSession.builder \
        .appName("POSStreamProcessor") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://bakery-warehouse/iceberg") \
        .config("spark.sql.streaming.checkpointLocation", "s3a://bakery-warehouse/checkpoints/pos") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

def process_pos_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Define schema for POS events
    pos_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_price", DoubleType(), True),
        StructField("customer_id", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("time_of_day", StringType(), True),
        StructField("is_member", BooleanType(), True),
        StructField("discount_applied", IntegerType(), True)
    ])
    
    # Read from Kafka
    kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "bakery-pos-events") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON data
    parsed_stream = kafka_stream.select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), pos_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Add processing metadata
    enriched_stream = parsed_stream \
        .withColumn("ingestion_time", current_timestamp()) \
        .withColumn("processing_status", lit("STREAMING")) \
        .withColumn("event_time", to_timestamp(col("event_time"))) \
        .withColumn("raw_payload", 
            to_json(struct([col(c) for c in parsed_stream.columns])))
    
    # Data quality checks
    validated_stream = enriched_stream.filter(
        (col("quantity") > 0) & 
        (col("unit_price") > 0) &
        (col("product_id").isNotNull()) &
        (col("store_id").isNotNull()) &
        (col("event_id").isNotNull())
    )
    
    # Write to Bronze layer (micro-batch)
    bronze_writer = validated_stream \
        .writeStream \
        .outputMode("append") \
        .format("iceberg") \
        .option("path", "local.bronze.sales_events") \
        .option("fanout-enabled", "true") \
        .option("checkpointLocation", "s3a://bakery-warehouse/checkpoints/pos-bronze") \
        .trigger(processingTime='30 seconds')
    
    # Real-time aggregations for monitoring
    sales_aggregates = validated_stream \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("store_id"),
            col("product_id")
        ) \
        .agg(
            sum("quantity").alias("total_quantity"),
            sum("total_price").alias("total_revenue"),
            count("event_id").alias("transaction_count"),
            avg("unit_price").alias("avg_price")
        )
    
    # Write aggregates for real-time dashboard
    dashboard_writer = sales_aggregates \
        .writeStream \
        .outputMode("update") \
        .format("iceberg") \
        .option("path", "local.bronze.sales_aggregates_realtime") \
        .option("checkpointLocation", "s3a://bakery-warehouse/checkpoints/pos-aggregates") \
        .trigger(processingTime='10 seconds')
    
    # Start streams
    bronze_query = bronze_writer.start()
    dashboard_query = dashboard_writer.start()
    
    # Alert on anomalies
    anomaly_stream = validated_stream.filter(
        (col("total_price") > 500) |  # High value transaction
        (col("quantity") > 50) |       # Large quantity
        (col("discount_applied") > 50) # High discount
    )
    
    def send_alert(df, epoch_id):
        """Send alerts for anomalous transactions"""
        if df.count() > 0:
            alerts = df.collect()
            for alert in alerts:
                print(f"ALERT: Anomalous transaction detected - "
                      f"Event: {alert.event_id}, "
                      f"Store: {alert.store_id}, "
                      f"Total: ${alert.total_price}")
                # In production, send to monitoring system
    
    anomaly_query = anomaly_stream \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(send_alert) \
        .trigger(processingTime='1 minute') \
        .start()
    
    # Wait for termination
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_pos_stream()