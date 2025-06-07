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
        StructField("timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("power_consumption", DoubleType(), True),
        StructField("vibration_level", DoubleType(), True),
        StructField("operational_status", StringType(), True),
        StructField("error_code", StringType(), True)
    ])
    
    # Read from Kafka
    kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "bakery-iot-events") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON data
    parsed_stream = kafka_stream.select(
        from_json(col("value").cast("string"), iot_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Enrich and validate
    enriched_stream = parsed_stream \
        .withColumn("event_time", to_timestamp(col("timestamp"))) \
        .withColumn("ingestion_time", current_timestamp()) \
        .withColumn("processing_status", lit("STREAMING")) \
        .withColumn("sensor_readings", 
            to_json(struct(
                col("temperature"),
                col("humidity"),
                col("vibration_level")
            ))
        ) \
        .withColumn("raw_payload", 
            to_json(struct([col(c) for c in parsed_stream.columns])))
    
    # Anomaly detection
    anomaly_conditions = (
        (col("temperature") > 300) |  # Overheating
        (col("temperature") < 100) |  # Too cold
        (col("power_consumption") > 150) |  # High power usage
        (col("vibration_level") > 10) |  # Excessive vibration
        (col("operational_status") == "ERROR") |
        (col("error_code").isNotNull())
    )
    
    # Split normal and anomaly streams
    normal_stream = enriched_stream.filter(~anomaly_conditions)
    anomaly_stream = enriched_stream.filter(anomaly_conditions)
    
    # Write normal data to bronze
    normal_writer = normal_stream \
        .writeStream \
        .outputMode("append") \
        .format("iceberg") \
        .option("path", "local.bronze.equipment_metrics") \
        .option("checkpointLocation", "s3a://bakery-warehouse/checkpoints/iot-bronze") \
        .trigger(processingTime='30 seconds')
    
    # Handle anomalies with immediate alerts
    def process_anomalies(df, epoch_id):
        """Process equipment anomalies"""
        if df.count() > 0:
            # Write to anomaly table
            df.write \
                .format("iceberg") \
                .mode("append") \
                .save("local.bronze.equipment_anomalies")
            
            # Generate alerts
            critical_alerts = df.filter(
                (col("temperature") > 350) |
                (col("operational_status") == "ERROR")
            ).collect()
            
            for alert in critical_alerts:
                print(f"CRITICAL ALERT: Equipment {alert.equipment_id} - "
                      f"Status: {alert.operational_status}, "
                      f"Temp: {alert.temperature}°C, "
                      f"Error: {alert.error_code}")
                # In production: send to alerting system
    
    anomaly_writer = anomaly_stream \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(process_anomalies) \
        .trigger(processingTime='10 seconds')
    
    # Real-time equipment monitoring aggregations
    equipment_stats = enriched_stream \
        .withWatermark("event_time", "5 minutes") \
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("equipment_id")
        ) \
        .agg(
            avg("temperature").alias("avg_temperature"),
            max("temperature").alias("max_temperature"),
            min("temperature").alias("min_temperature"),
            avg("power_consumption").alias("avg_power_consumption"),
            sum(when(col("operational_status") == "RUNNING", 1)
                .otherwise(0)).alias("running_minutes"),
            count(when(anomaly_conditions, 1)).alias("anomaly_count")
        )
    
    # Write real-time stats
    stats_writer = equipment_stats \
        .writeStream \
        .outputMode("update") \
        .format("iceberg") \
        .option("path", "local.bronze.equipment_stats_realtime") \
        .option("checkpointLocation", "s3a://bakery-warehouse/checkpoints/iot-stats") \
        .trigger(processingTime='30 seconds')
    
    # Predictive maintenance triggers
    maintenance_stream = equipment_stats.filter(
        (col("avg_temperature") > 280) |
        (col("anomaly_count") > 5) |
        (col("running_minutes") < 30)  # Running less than 50% of time
    )
    
    def trigger_maintenance_check(df, epoch_id):
        """Trigger maintenance workflows"""
        if df.count() > 0:
            maintenance_alerts = df.collect()
            for alert in maintenance_alerts:
                print(f"MAINTENANCE ALERT: Equipment {alert.equipment_id} - "
                      f"Avg Temp: {alert.avg_temperature}°C, "
                      f"Anomalies: {alert.anomaly_count}")
                
                # Create maintenance ticket
                maintenance_record = {
                    "equipment_id": alert.equipment_id,
                    "trigger_time": alert.window.start,
                    "avg_temperature": alert.avg_temperature,
                    "anomaly_count": alert.anomaly_count,
                    "priority": "HIGH" if alert.avg_temperature > 300 else "MEDIUM"
                }
                
                # In production: integrate with maintenance system
    
    maintenance_writer = maintenance_stream \
        .writeStream \
        .outputMode("update") \
        .foreachBatch(trigger_maintenance_check) \
        .trigger(processingTime='5 minutes')
    
    # Start all streams
    normal_query = normal_writer.start()
    anomaly_query = anomaly_writer.start()
    stats_query = stats_writer.start()
    maintenance_query = maintenance_writer.start()
    
    # Wait for termination
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_iot_stream()