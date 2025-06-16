from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

def create_spark_session():
    """Create Spark session for feedback streaming"""
    return SparkSession.builder \
        .appName("FeedbackStreamProcessor") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://bakery-warehouse/iceberg") \
        .config("spark.sql.streaming.checkpointLocation", "s3a://bakery-warehouse/checkpoints/feedback") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

def process_feedback_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Define schema for feedback data
    feedback_schema = StructType([
        StructField("feedback_id", StringType(), True),
        StructField("feedback_time", StringType(), True),
        StructField("ingestion_time", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("platform", StringType(), True),
        StructField("review_text", StringType(), True),
        StructField("sentiment", StringType(), True),
        StructField("verified_purchase", BooleanType(), True),
        StructField("helpful_count", IntegerType(), True)
    ])
    
    # Read from Kafka
    kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "bakery-customer-feedback") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON data
    parsed_stream = kafka_stream.select(
        from_json(col("value").cast("string"), feedback_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Create raw_payload as JSON
    parsed_with_payload = parsed_stream.withColumn(
        "raw_payload", 
        to_json(struct(
            col("sentiment"), 
            col("verified_purchase"), 
            col("helpful_count")
        ))
    )
    
    # Ensure data conforms to bronze schema
    bronze_stream = parsed_with_payload \
        .withColumn("feedback_time", to_timestamp(col("feedback_time"))) \
        .withColumn("ingestion_time", when(col("ingestion_time").isNull(), current_timestamp())
                    .otherwise(to_timestamp(col("ingestion_time")))) \
        .withColumn("processing_status", lit("STREAMING"))
    
    # Data validation
    validated_stream = bronze_stream.filter(
        col("feedback_id").isNotNull() &
        col("product_id").isNotNull() &
        col("rating").isNotNull()
    )
    
    # Select fields in the same order as bronze_customer_feedback schema
    final_stream = validated_stream.select(
        "feedback_id", "feedback_time", "ingestion_time", "customer_id", "product_id", 
        "rating", "platform", "review_text", "raw_payload", "processing_status"
    )
    
    # Write to bronze layer
    query = final_stream \
        .writeStream \
        .outputMode("append") \
        .format("iceberg") \
        .option("path", "local.db.bronze_customer_feedback") \
        .option("checkpointLocation", "s3a://bakery-warehouse/checkpoints/bronze_customer_feedback") \
        .trigger(processingTime='30 seconds') \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    process_feedback_stream()