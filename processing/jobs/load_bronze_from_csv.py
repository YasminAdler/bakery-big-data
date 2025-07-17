"""
Load bronze_combined.csv into Iceberg Bronze tables
Run once after 'make start && make init'
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, lit, when, concat_ws, struct, to_json
from pyspark.sql.types import *

spark = (SparkSession.builder
         .appName("Load Bronze From CSV")  # type: ignore[attr-defined]
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.local.type", "hadoop")
         .config("spark.sql.catalog.local.warehouse", "s3a://iceberg-warehouse/")
         .getOrCreate())

print("Loading bronze_combined.csv...")

csv_path = "/opt/spark-apps/data/bronze_combined.csv"
df = spark.read.option("header", "true").csv(csv_path)

# Convert timestamp columns
df = (df
      .withColumn("event_time", to_timestamp("event_time"))
      .withColumn("feedback_time", to_timestamp("feedback_time"))
      .withColumn("ingestion_time", to_timestamp("ingestion_time"))
      .withColumn("Date", to_date("Date")))

print("Processing Sales Events...")
# Process Sales Events - exact schema match
df_sales = (df.filter(col("event_id").isNotNull())
            .select(
                col("event_id").cast("string"),
                col("event_time").cast("timestamp"),
                col("ingestion_time").cast("timestamp"),
                col("product_id").cast("int"),
                col("store_id").cast("int"),
                col("quantity").cast("int"),
                col("unit_price").cast("decimal(10,2)"),
                col("customer_id").cast("string"),
                col("Date").alias("date").cast("date"),
                col("`raw_payload.time_of_day`").alias("time_of_day").cast("string"),
                when(col("processing_status").isNull(), lit("pending"))
                .otherwise(col("processing_status")).alias("processing_status").cast("string")
            ))

df_sales.write.format("iceberg").mode("append").save("local.bronze.sales_events")
print(f"✓ Sales events loaded: {df_sales.count()} records")

print("Processing Inventory Updates...")
# Process Inventory Updates - exact schema match
df_inventory = (df.filter(col("update_id").isNotNull())
                .select(
                    col("update_id").cast("string"),
                    col("event_time").cast("timestamp"),
                    col("ingestion_time").cast("timestamp"),
                    col("product_id").cast("int"),
                    col("store_id").cast("int"),
                    col("beginning_stock").cast("int"),
                    col("restocked_quantity").cast("int"),
                    col("sold_quantity").cast("int"),
                    col("waste_quantity").cast("int"),
                    col("reported_by").cast("string"),
                    when(col("processing_status").isNull(), lit("pending"))
                    .otherwise(col("processing_status")).alias("processing_status").cast("string"),
                    col("`raw_payload.delay_hours`").alias("late_arrival_hours").cast("double")
                ))

df_inventory.write.format("iceberg").mode("append").save("local.bronze.inventory_updates")
print(f"✓ Inventory updates loaded: {df_inventory.count()} records")

print("Processing Equipment Metrics...")
# Process Equipment Metrics - exact schema match
df_equipment = (df.filter(col("metric_id").isNotNull())
                .select(
                    col("metric_id").cast("string"),
                    col("equipment_id").cast("int"),
                    col("event_time").cast("timestamp"),
                    col("ingestion_time").cast("timestamp"),
                    col("power_consumption").cast("decimal(8,2)"),
                    col("operational_status").cast("string"),
                    # Create raw_payload from available raw_payload columns
                    to_json(struct(
                        col("`raw_payload.equipment_name`").alias("equipment_name"),
                        col("`raw_payload.equipment_type`").alias("equipment_type"),
                        col("`raw_payload.temperature`").alias("temperature"),
                        col("`raw_payload.humidity`").alias("humidity"),
                        col("`raw_payload.vibration_level`").alias("vibration_level"),
                        col("`raw_payload.error_code`").alias("error_code"),
                        col("`raw_payload.runtime_minutes`").alias("runtime_minutes"),
                        col("`raw_payload.total_runtime_hours`").alias("total_runtime_hours"),
                        col("`raw_payload.efficiency`").alias("efficiency"),
                        col("`raw_payload.stress_level`").alias("stress_level"),
                        col("`raw_payload.cycles_since_maintenance`").alias("cycles_since_maintenance")
                    )).alias("raw_payload").cast("string"),
                    when(col("processing_status").isNull(), lit("pending"))
                    .otherwise(col("processing_status")).alias("processing_status").cast("string")
                ))

df_equipment.write.format("iceberg").mode("append").save("local.bronze.equipment_metrics")
print(f"✓ Equipment metrics loaded: {df_equipment.count()} records")

print("Processing Customer Feedback...")
# Process Customer Feedback - exact schema match
df_feedback = (df.filter(col("feedback_id").isNotNull())
               .select(
                   col("feedback_id").cast("string"),
                   col("feedback_time").cast("timestamp"),
                   col("ingestion_time").cast("timestamp"),
                   col("customer_id").cast("string"),
                   col("product_id").cast("int"),
                   col("rating").cast("int"),
                   col("platform").cast("string"),
                   col("review_text").cast("string"),
                   # Create raw_payload from available raw_payload columns
                   to_json(struct(
                       col("`raw_payload.full_review`").alias("full_review"),
                       col("`raw_payload.sentiment`").alias("sentiment"),
                       col("`raw_payload.verified_purchase`").alias("verified_purchase"),
                       col("`raw_payload.helpful_count`").alias("helpful_count"),
                       col("`raw_payload.response_from_owner`").alias("response_from_owner"),
                       col("`raw_payload.language`").alias("language"),
                       col("`raw_payload.location`").alias("location")
                   )).alias("raw_payload").cast("string"),
                   when(col("processing_status").isNull(), lit("pending"))
                   .otherwise(col("processing_status")).alias("processing_status").cast("string")
               ))

df_feedback.write.format("iceberg").mode("append").save("local.bronze.customer_feedback")
print(f"✓ Customer feedback loaded: {df_feedback.count()} records")

print("✓ CSV loaded into Bronze tables successfully!")
spark.stop()