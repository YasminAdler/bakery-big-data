from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

def create_spark_session():
    """Create Spark session for IoT streaming"""
    return SparkSession.builder \
        .appName("IoTStreamProcessor") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://bakery-warehouse/iceberg") \
        .config("spark.sql.streaming.checkpointLocation", "s3a://bakery-warehouse/checkpoints/iot") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

def process_iot_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Define schema for IoT events
    iot_schema = StructType([
        StructField("metric_id", StringType(), True),
        StructField("equipment_id", IntegerType(), True),
        StructField("event_time", StringType(), True),
        StructField("ingestion_time", StringType(), True),
        StructField("power_consumption", DecimalType(8,2), True),
        StructField("operational_status", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("vibration_level", DoubleType(), True),
        StructField("error_code", StringType(), True)
    ])
    
    # Read from Kafka
    kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "bakery-iot-events") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON data
    parsed_stream = kafka_stream.select(
        from_json(col("value").cast("string"), iot_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Create raw_payload as JSON
    parsed_with_payload = parsed_stream.withColumn(
        "raw_payload", 
        to_json(struct(
            col("temperature"), 
            col("humidity"), 
            col("vibration_level"), 
            col("error_code")
        ))
    )
    
    # Ensure data conforms to bronze schema
    bronze_stream = parsed_with_payload \
        .withColumn("event_time", to_timestamp(col("event_time"))) \
        .withColumn("ingestion_time", when(col("ingestion_time").isNull(), current_timestamp())
                    .otherwise(to_timestamp(col("ingestion_time")))) \
        .withColumn("processing_status", lit("STREAMING"))
    
    # Data validation
    validated_stream = bronze_stream.filter(
        col("metric_id").isNotNull() &
        col("equipment_id").isNotNull() &
        col("event_time").isNotNull() &
        col("power_consumption").isNotNull()
    )
    
    # Select fields in the same order as bronze_equipment_metrics schema
    final_stream = validated_stream.select(
        "metric_id", "equipment_id", "event_time", "ingestion_time", 
        "power_consumption", "operational_status", "raw_payload", "processing_status"
    )
    
    # Write to bronze layer
    query = final_stream \
        .writeStream \
        .outputMode("append") \
        .format("iceberg") \
        .option("path", "local.db.bronze_equipment_metrics") \
        .option("checkpointLocation", "s3a://bakery-warehouse/checkpoints/bronze_equipment_metrics") \
        .trigger(processingTime='30 seconds') \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    process_iot_stream()