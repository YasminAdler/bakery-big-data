from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

def create_spark_session():
    """Create Spark session for batch processing"""
    return SparkSession.builder \
        .appName("BatchProcessor") \
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

def process_promotions():
    """Process promotions batch data"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Define schema for promotions
    promotions_schema = StructType([
        StructField("promo_id", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("promo_type", StringType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("discount_percentage", DecimalType(5,2), True),
        StructField("target_audience", StringType(), True),
        StructField("min_quantity", IntegerType(), True),
        StructField("max_quantity", IntegerType(), True),
        StructField("terms_conditions", StringType(), True)
    ])
    
    # Read from Kafka
    kafka_batch = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "bakery-promotions") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    # Process if data exists
    if not kafka_batch.isEmpty():
        # Parse JSON data
        parsed_batch = kafka_batch.select(
            from_json(col("value").cast("string"), promotions_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        # Create raw_payload
        parsed_with_payload = parsed_batch.withColumn(
            "raw_payload", 
            to_json(struct(
                col("target_audience"),
                col("min_quantity"),
                col("max_quantity"),
                col("terms_conditions")
            ))
        )
        
        # Select fields in the same order as bronze_promotions schema
        final_batch = parsed_with_payload.select(
            "promo_id", "product_id", "promo_type", "start_date", "end_date", 
            "discount_percentage", "raw_payload", lit("BATCH").alias("processing_status")
        )
        
        # Write to bronze layer
        final_batch.write \
            .format("iceberg") \
            .mode("append") \
            .save("local.db.bronze_promotions")
        
        print(f"Processed {final_batch.count()} promotion records")

def process_weather():
    """Process weather batch data"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Define schema for weather data
    weather_schema = StructType([
        StructField("weather_id", StringType(), True),
        StructField("date", DateType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("temperature_celsius", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("wind_speed", IntegerType(), True),
        StructField("precipitation_mm", DoubleType(), True),
        StructField("uv_index", IntegerType(), True)
    ])
    
    # Read from Kafka
    kafka_batch = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "bakery-weather-data") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    # Process if data exists
    if not kafka_batch.isEmpty():
        # Parse JSON data
        parsed_batch = kafka_batch.select(
            from_json(col("value").cast("string"), weather_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        # Create raw_payload
        parsed_with_payload = parsed_batch.withColumn(
            "raw_payload", 
            to_json(struct(
                col("temperature_celsius"),
                col("humidity"),
                col("wind_speed"),
                col("precipitation_mm"),
                col("uv_index")
            ))
        )
        
        # Select fields in the same order as bronze_weather_data schema
        final_batch = parsed_with_payload.select(
            "weather_id", "date", "store_id", "weather_condition", 
            "raw_payload", lit("BATCH").alias("processing_status")
        )
        
        # Write to bronze layer
        final_batch.write \
            .format("iceberg") \
            .mode("append") \
            .save("local.db.bronze_weather_data")
        
        print(f"Processed {final_batch.count()} weather records")

def main():
    """Run batch processors on a schedule"""
    while True:
        try:
            # Process promotions
            process_promotions()
            
            # Process weather
            process_weather()
            
            # Wait for next batch
            print("Batch processing complete. Waiting for next run...")
            time.sleep(3600)  # Run hourly
            
        except Exception as e:
            print(f"Error in batch processing: {str(e)}")
            time.sleep(300)  # Wait 5 minutes before retry

if __name__ == "__main__":
    main()