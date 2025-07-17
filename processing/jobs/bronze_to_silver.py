#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    return SparkSession.builder \
        .appName("Bronze to Silver ETL") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://iceberg-warehouse/") \
        .getOrCreate()


def calculate_data_quality_score(df, checks):
    df = df.withColumn("data_quality_score", lit(100))
    
    for check_name, check_expr, penalty in checks:
        df = df.withColumn(
            "data_quality_score",
            when(check_expr, col("data_quality_score")).otherwise(col("data_quality_score") - penalty)
        )
        df = df.withColumn(f"dq_check_{check_name}", check_expr)
    
    return df


def process_sales_to_silver(spark, process_date):
    logger.info(f"Processing sales data for date: {process_date}")
    
    bronze_sales = spark.sql(f"""
        SELECT * FROM local.bronze.sales_events
        WHERE date = '{process_date}'
        AND processing_status = 'pending'
    """)
    
    quality_checks = [
        ("valid_product_id", col("product_id").between(1, 10), 20),
        ("valid_store_id", col("store_id").between(1, 5), 20),
        ("positive_quantity", col("quantity") > 0, 25),
        ("positive_price", col("unit_price") > 0, 25),
        ("valid_time_of_day", col("time_of_day").isin("morning", "lunch", "afternoon", "evening"), 10)
    ]
    
    silver_sales = bronze_sales \
        .withColumn("sale_id", col("event_id")) \
        .withColumn("sale_date", col("date")) \
        .withColumn("sale_time", date_format(col("event_time"), "HH:mm:ss")) \
        .withColumn("quantity_sold", col("quantity")) \
        .withColumn("total_revenue", col("quantity") * col("unit_price")) \
        .withColumn("promo_id", when(col("unit_price") < 5.0, lit("PROMO_STANDARD")).otherwise(lit(None))) \
        .withColumn("weather_id", lit(None)) \
        .withColumn("source_system", lit("kafka_stream")) \
        .withColumn("etl_timestamp", current_timestamp())
    
    # Apply data quality scoring
    silver_sales = calculate_data_quality_score(silver_sales, quality_checks)
    
    # Select final columns
    final_silver_sales = silver_sales.select(
        "sale_id", "product_id", "store_id", "customer_id",
        "sale_date", "sale_time", "time_of_day", "quantity_sold",
        "unit_price", "total_revenue", "promo_id", "weather_id",
        "data_quality_score", "source_system", "etl_timestamp"
    )
    
    # Write to Silver layer (merge for deduplication)
    final_silver_sales.createOrReplaceTempView("sales_updates")
    
    spark.sql("""
        MERGE INTO local.silver.sales t
            USING sales_updates s
    ON t.sale_id = s.sale_id
    WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
    UPDATE local.bronze.sales_events
    SET processing_status = 'processed'
    WHERE date = '{process_date}'
    AND processing_status = 'pending'
""")

record_count = final_silver_sales.count()
logger.info(f"Processed {record_count} sales records to Silver layer")

return record_count


def process_inventory_to_silver(spark, process_date):
logger.info(f"Processing inventory data for date: {process_date}")

bronze_inventory = spark.sql(f"""
    SELECT * FROM local.bronze.inventory_updates
    WHERE date(event_time) = '{process_date}'
    AND processing_status = 'pending'
    UNION ALL
    SELECT * FROM local.bronze.inventory_updates
    WHERE date(event_time) = '{process_date}'
    AND processing_status = 'processed'
    AND ingestion_time > date_sub(current_timestamp(), 2)
    AND late_arrival_hours > 0
""")

quality_checks = [
    ("valid_product_id", col("product_id").between(1, 10), 15),
    ("valid_store_id", col("store_id").between(1, 5), 15),
    ("non_negative_stock", col("beginning_stock") >= 0, 20),
    ("non_negative_restock", col("restocked_quantity") >= 0, 20),
    ("non_negative_sold", col("sold_quantity") >= 0, 15),
    ("non_negative_waste", col("waste_quantity") >= 0, 15)
]
    silver_inventory = bronze_inventory \
        .withColumn("inventory_id", col("update_id")) \
        .withColumn("inventory_date", to_date(col("event_time"))) \
        .withColumn("waste_ratio", 
            when(col("beginning_stock") + col("restocked_quantity") > 0,
                 col("waste_quantity") / (col("beginning_stock") + col("restocked_quantity"))
            ).otherwise(0)) \
        .withColumn("closing_stock", 
            col("beginning_stock") + col("restocked_quantity") - col("sold_quantity") - col("waste_quantity")) \
        .withColumn("days_of_supply", 
            when(col("sold_quantity") > 0,
                 col("closing_stock") / col("sold_quantity")
            ).otherwise(999)) \
        .withColumn("source_system", lit("kafka_stream")) \
        .withColumn("etl_timestamp", current_timestamp())
    
    # Apply data quality scoring
    silver_inventory = calculate_data_quality_score(silver_inventory, quality_checks)
    
    # Select final columns
    final_silver_inventory = silver_inventory.select(
        "inventory_id", "product_id", "store_id", "inventory_date",
        "beginning_stock", "restocked_quantity", "sold_quantity", "waste_quantity",
        "waste_ratio", "closing_stock", "days_of_supply",
        "data_quality_score", "source_system", "etl_timestamp"
    )
    
    # Handle updates (merge with existing data)
    final_silver_inventory.createOrReplaceTempView("inventory_updates")
    
    spark.sql("""
        MERGE INTO local.silver.inventory t
        USING inventory_updates s
        ON t.inventory_id = s.inventory_id
        WHEN MATCHED AND s.etl_timestamp > t.etl_timestamp THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    # Update processing status
    spark.sql(f"""
        UPDATE local.bronze.inventory_updates
        SET processing_status = 'processed'
        WHERE date(event_time) = '{process_date}'
        AND processing_status = 'pending'
    """)
    
    record_count = final_silver_inventory.count()
    logger.info(f"Processed {record_count} inventory records to Silver layer")
    
    return record_count


def process_equipment_metrics_to_silver(spark, process_date):
    """Process equipment metrics from Bronze to Silver"""
    logger.info(f"Processing equipment metrics for date: {process_date}")
    
    # Read from Bronze
    bronze_metrics = spark.sql(f"""
        SELECT * FROM local.bronze.equipment_metrics
        WHERE date(event_time) = '{process_date}'
        AND processing_status = 'pending'
    """)
    
    # Data quality checks
    quality_checks = [
        ("valid_equipment_id", col("equipment_id").between(1, 10), 25),
        ("valid_power", col("power_consumption") >= 0, 25),
        ("valid_status", col("operational_status").isNotNull(), 25),
        ("reasonable_power", col("power_consumption") < 100, 25)
    ]
    
    # Calculate operational hours per equipment per day
    window_spec = Window.partitionBy("equipment_id", to_date("event_time")).orderBy("event_time")
    
    silver_metrics = bronze_metrics \
        .withColumn("metric_date", to_date(col("event_time"))) \
        .withColumn("metric_time", date_format(col("event_time"), "HH:mm:ss")) \
        .withColumn("operational_hours",
            when(col("operational_status") == "running", 
                 (unix_timestamp(lead("event_time").over(window_spec)) - unix_timestamp(col("event_time"))) / 3600
            ).otherwise(0)) \
        .withColumn("maintenance_alert",
            when((col("operational_status") == "error") | 
                 (col("operational_status") == "maintenance"), True).otherwise(False)) \
        .withColumn("source_system", lit("iot_sensors")) \
        .withColumn("etl_timestamp", current_timestamp())
    
    # Apply data quality scoring
    silver_metrics = calculate_data_quality_score(silver_metrics, quality_checks)
    
    # Select final columns
    final_silver_metrics = silver_metrics.select(
        "metric_id", "equipment_id", "metric_date", "metric_time",
        "power_consumption", "operational_status", "operational_hours",
        "maintenance_alert", "data_quality_score", "source_system", "etl_timestamp"
    )
    
    # Write to Silver
    final_silver_metrics.write \
        .mode("append") \
        .saveAsTable("local.silver.equipment_metrics")
    
    # Update processing status
    spark.sql(f"""
        UPDATE local.bronze.equipment_metrics
        SET processing_status = 'processed'
        WHERE date(event_time) = '{process_date}'
        AND processing_status = 'pending'
    """)
    
    record_count = final_silver_metrics.count()
    logger.info(f"Processed {record_count} equipment metrics to Silver layer")
    
    return record_count


def generate_data_quality_report(spark, process_date):
    """Generate data quality report for the processed data"""
    logger.info("Generating data quality report...")
    
    # Sales quality summary
    sales_quality = spark.sql(f"""
        SELECT 
            'sales' as table_name,
            COUNT(*) as total_records,
            AVG(data_quality_score) as avg_quality_score,
            SUM(CASE WHEN data_quality_score = 100 THEN 1 ELSE 0 END) as perfect_records,
            SUM(CASE WHEN data_quality_score < 50 THEN 1 ELSE 0 END) as poor_quality_records
        FROM local.silver.sales
        WHERE sale_date = '{process_date}'
    """)
    
    # Inventory quality summary  
    inventory_quality = spark.sql(f"""
        SELECT 
            'inventory' as table_name,
            COUNT(*) as total_records,
            AVG(data_quality_score) as avg_quality_score,
            SUM(CASE WHEN data_quality_score = 100 THEN 1 ELSE 0 END) as perfect_records,
            SUM(CASE WHEN data_quality_score < 50 THEN 1 ELSE 0 END) as poor_quality_records
        FROM local.silver.inventory
        WHERE inventory_date = '{process_date}'
    """)
    
    # Equipment metrics quality summary
    metrics_quality = spark.sql(f"""
        SELECT 
            'equipment_metrics' as table_name,
            COUNT(*) as total_records,
            AVG(data_quality_score) as avg_quality_score,
            SUM(CASE WHEN data_quality_score = 100 THEN 1 ELSE 0 END) as perfect_records,
            SUM(CASE WHEN data_quality_score < 50 THEN 1 ELSE 0 END) as poor_quality_records
        FROM local.silver.equipment_metrics
        WHERE metric_date = '{process_date}'
    """)
    
    # Combine reports
    quality_report = sales_quality.union(inventory_quality).union(metrics_quality)
    
    logger.info("Data Quality Report:")
    quality_report.show()
    
    return quality_report


def main():
    """Main execution function"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Get the most recent date with data instead of assuming today
    latest_date_result = spark.sql("""
        SELECT MAX(date) as latest_date 
        FROM local.bronze.sales_events 
        WHERE date IS NOT NULL
    """).collect()
    
    if latest_date_result and latest_date_result[0]["latest_date"]:
        process_date = latest_date_result[0]["latest_date"].strftime("%Y-%m-%d")
    else:
        process_date = datetime.now().strftime("%Y-%m-%d")
    
    logger.info(f"Processing date determined as: {process_date}")
    
    try:
        logger.info(f"Starting Bronze to Silver ETL for date: {process_date}")
        
        # Process each data type
        sales_count = process_sales_to_silver(spark, process_date)
        inventory_count = process_inventory_to_silver(spark, process_date)
        metrics_count = process_equipment_metrics_to_silver(spark, process_date)
        
        # Generate quality report
        quality_report = generate_data_quality_report(spark, process_date)
        
        logger.info(f"Bronze to Silver ETL completed successfully!")
        logger.info(f"Total records processed - Sales: {sales_count}, Inventory: {inventory_count}, Equipment: {metrics_count}")
        
    except Exception as e:
        logger.error(f"Error in Bronze to Silver ETL: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main() 