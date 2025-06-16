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
        StructField("ingestion_time", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10,2), True),
        StructField("customer_id", StringType(), True),
        StructField("Date", DateType(), True),
        StructField("processing_status", StringType(), True),
        StructField("raw_payload", StringType(), True)
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
        from_json(col("value").cast("string"), pos_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Ensure data conforms to bronze schema
    bronze_stream = parsed_stream \
        .withColumn("event_time", to_timestamp(col("event_time"))) \
        .withColumn("ingestion_time", when(col("ingestion_time").isNull(), current_timestamp())
                    .otherwise(to_timestamp(col("ingestion_time")))) \
        .withColumn("Date", when(col("Date").isNull(), to_date(col("event_time")))
                    .otherwise(col("Date"))) \
        .withColumn("processing_status", when(col("processing_status").isNull(), lit("STREAMING"))
                    .otherwise(col("processing_status")))
    
    # Data validation
    validated_stream = bronze_stream.filter(
        col("event_id").isNotNull() &
        col("product_id").isNotNull() &
        col("store_id").isNotNull() &
        col("quantity").isNotNull() &
        col("unit_price").isNotNull()
    )
    
    # Select fields in the same order as bronze_sales_events schema
    final_stream = validated_stream.select(
        "event_id", "event_time", "ingestion_time", "product_id", "store_id", 
        "quantity", "unit_price", "customer_id", "Date", "processing_status"
    )
    
    # Write to bronze layer
    query = final_stream \
        .writeStream \
        .outputMode("append") \
        .format("iceberg") \
        .option("path", "local.db.bronze_sales_events") \
        .option("checkpointLocation", "s3a://bakery-warehouse/checkpoints/bronze_sales_events") \
        .trigger(processingTime='30 seconds') \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    process_pos_stream()