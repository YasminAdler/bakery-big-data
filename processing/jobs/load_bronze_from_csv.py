"""
Load bronze_combined.csv into Iceberg Bronze tables
Run once after 'make start && make init'
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date

spark = (SparkSession.builder  # type: ignore[attr-defined]
         .appName("Load Bronze From CSV")  # type: ignore[attr-defined]
         .config("spark.sql.extensions",
                 "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.local.type", "hadoop")
         .config("spark.sql.catalog.local.warehouse", "s3a://iceberg-warehouse/")
         .getOrCreate())

csv_path = "/opt/spark-apps/data/bronze_combined.csv"
df = (spark.read.option("header", "true").csv(csv_path))

df = (df
       .withColumn("event_time",   to_timestamp("event_time"))
       .withColumn("feedback_time", to_timestamp("feedback_time"))
       .withColumn("ingestion_time", to_timestamp("ingestion_time"))
       .withColumn("Date",          to_date("Date")))

sales_cols = ["event_id", "event_time", "ingestion_time",
              "product_id", "store_id", "quantity", "unit_price",
              "customer_id", "Date", "`raw_payload.time_of_day`",
              "processing_status"]

df_sales = (df.filter(col("event_id").isNotNull())
              .select(sales_cols)
              .withColumnRenamed("Date", "date")
              .withColumnRenamed("`raw_payload.time_of_day`", "time_of_day"))

(df_sales.write
        .format("iceberg")
        .mode("append")
        .save("local.bronze.sales_events"))

inv_cols = ["update_id", "event_time", "ingestion_time",
            "product_id", "store_id", "beginning_stock",
            "restocked_quantity", "sold_quantity", "waste_quantity",
            "reported_by", "processing_status",
            "`raw_payload.delay_hours`"]

df_inv = (df.filter(col("update_id").isNotNull())
            .select(inv_cols)
            .withColumnRenamed("`raw_payload.delay_hours`", "late_arrival_hours"))

(df_inv.write
       .format("iceberg")
       .mode("append")
       .save("local.bronze.inventory_updates"))

metric_cols = ["metric_id", "equipment_id", "event_time",
               "ingestion_time", "power_consumption",
               "operational_status", "processing_status"]

df_metrics = (df.filter(col("metric_id").isNotNull())
                .select(metric_cols))

(df_metrics.write
         .format("iceberg")
         .mode("append")
         .save("local.bronze.equipment_metrics"))

print("✓ CSV loaded into Bronze tables")
spark.stop()