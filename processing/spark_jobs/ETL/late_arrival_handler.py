from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
from datetime import datetime, timedelta

def create_spark_session():
    """Create Spark session with Iceberg configuration"""
    return SparkSession.builder \
        .appName("LateArrivalHandler") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://bakery-warehouse/iceberg") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def handle_late_inventory_data(spark, process_date, window_hours):
    """Handle late-arriving inventory data within specified window"""
    
    cutoff_time = datetime.strptime(process_date, "%Y-%m-%d") - timedelta(hours=int(window_hours))
    
    # Identify late arrivals
    late_arrivals = spark.read \
        .format("iceberg") \
        .load("local.bronze.inventory_updates") \
        .filter(
            (col("event_time") >= cutoff_time) & 
            (col("event_time") < process_date) &
            (col("processing_status") == "PENDING") &
            (datediff(col("ingestion_time"), col("event_time")) > 0)
        )
    
    if late_arrivals.count() > 0:
        print(f"Found {late_arrivals.count()} late-arriving inventory records")
        
        # Mark as processing
        late_arrivals = late_arrivals.withColumn(
            "processing_status", lit("PROCESSING")
        ).withColumn(
            "is_late_arrival", lit(True)
        ).withColumn(
            "processing_timestamp", current_timestamp()
        )
        
        # Reprocess through silver layer
        silver_df = late_arrivals.select(
            col("update_id").alias("inventory_id"),
            col("product_id"),
            col("store_id"),
            to_date(col("event_time")).alias("inventory_date"),
            col("beginning_stock"),
            col("restocked_quantity"),
            col("sold_quantity"),
            col("waste_quantity"),
            when(col("sold_quantity") > 0, 
                 col("waste_quantity") / col("sold_quantity")
            ).otherwise(0).alias("waste_ratio"),
            (col("beginning_stock") + col("restocked_quantity") - 
             col("sold_quantity") - col("waste_quantity")).alias("closing_stock"),
            lit(90).alias("data_quality_score"),  # Lower score for late data
            lit("INVENTORY_LATE").alias("source_system"),
            current_timestamp().alias("etl_timestamp"),
            col("is_late_arrival")
        )
        
        # Check for conflicts with existing data
        existing_silver = spark.read \
            .format("iceberg") \
            .load("local.silver.inventory") \
            .filter(
                col("inventory_date").isin(
                    silver_df.select("inventory_date").distinct().collect()
                )
            )
        
        # Resolve conflicts - late data takes precedence for same product/store/date
        conflicts = silver_df.alias("new").join(
            existing_silver.alias("old"),
            ["product_id", "store_id", "inventory_date"],
            "inner"
        )
        
        if conflicts.count() > 0:
            print(f"Resolving {conflicts.count()} conflicts")
            
            # Archive conflicting records
            conflicts_to_archive = conflicts.select("old.*") \
                .withColumn("archived_timestamp", current_timestamp()) \
                .withColumn("archive_reason", lit("LATE_ARRIVAL_OVERRIDE"))
            
            conflicts_to_archive.write \
                .format("iceberg") \
                .mode("append") \
                .save("local.bronze.inventory_archive")
            
            # Delete conflicting records from silver
            spark.sql("""
                DELETE FROM local.silver.inventory
                WHERE (product_id, store_id, inventory_date) IN (
                    SELECT product_id, store_id, inventory_date
                    FROM local.silver.inventory_conflicts_temp
                )
            """)
        
        # Insert late arrivals into silver
        silver_df.write \
            .format("iceberg") \
            .mode("append") \
            .save("local.silver.inventory")
        
        # Update bronze records as processed
        spark.sql("""
            UPDATE local.bronze.inventory_updates
            SET processing_status = 'COMPLETED',
                processing_timestamp = current_timestamp()
            WHERE update_id IN ({})
        """.format(
            ",".join([f"'{id}'" for id in late_arrivals.select("update_id").collect()])
        ))
        
        # Trigger downstream updates
        update_downstream_tables(spark, silver_df)
        
        return silver_df.count()
    
    return 0

def update_downstream_tables(spark, updated_inventory):
    """Update gold layer tables affected by late arrivals"""
    
    affected_dates = updated_inventory.select("inventory_date").distinct().collect()
    affected_products = updated_inventory.select("product_id").distinct().collect()
    affected_stores = updated_inventory.select("store_id").distinct().collect()
    
    # Recalculate inventory facts
    for row in affected_dates:
        date = row.inventory_date
        
        # Update fact_inventory
        spark.sql(f"""
            MERGE INTO local.gold.fact_inventory AS target
            USING (
                SELECT * FROM local.silver.inventory
                WHERE inventory_date = '{date}'
                AND product_id IN ({','.join([str(p.product_id) for p in affected_products])})
                AND store_id IN ({','.join([str(s.store_id) for s in affected_stores])})
            ) AS source
            ON target.inventory_id = source.inventory_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        # Update ML features that depend on inventory
        spark.sql(f"""
            UPDATE local.gold.fact_demand_forecast_features
            SET inventory_level = (
                SELECT closing_stock 
                FROM local.silver.inventory i
                WHERE i.product_id = fact_demand_forecast_features.product_id
                AND i.store_id = fact_demand_forecast_features.store_id
                AND i.inventory_date = fact_demand_forecast_features.date
            ),
            waste_ratio = (
                SELECT waste_ratio
                FROM local.silver.inventory i
                WHERE i.product_id = fact_demand_forecast_features.product_id
                AND i.store_id = fact_demand_forecast_features.store_id
                AND i.inventory_date = fact_demand_forecast_features.date
            )
            WHERE date = '{date}'
            AND product_id IN ({','.join([str(p.product_id) for p in affected_products])})
            AND store_id IN ({','.join([str(s.store_id) for s in affected_stores])})
        """)

def handle_late_feedback_data(spark, process_date, window_hours):
    """Handle late-arriving customer feedback"""
    
    cutoff_time = datetime.strptime(process_date, "%Y-%m-%d") - timedelta(hours=int(window_hours))
    
    late_feedback = spark.read \
        .format("iceberg") \
        .load("local.bronze.customer_feedback") \
        .filter(
            (col("feedback_time") >= cutoff_time) & 
            (col("feedback_time") < process_date) &
            (col("processing_status") == "PENDING") &
            (datediff(col("ingestion_time"), col("feedback_time")) > 0)
        )
    
    if late_feedback.count() > 0:
        print(f"Found {late_feedback.count()} late-arriving feedback records")
        
        # Process late feedback similar to inventory
        # ... (implementation similar to inventory handling)
        
        return late_feedback.count()
    
    return 0

def generate_late_arrival_report(spark, process_date):
    """Generate report on late arrivals"""
    
    report = spark.sql(f"""
        WITH late_arrivals AS (
            SELECT 
                'inventory' as data_type,
                COUNT(*) as record_count,
                AVG(DATEDIFF(ingestion_time, event_time)) as avg_delay_days,
                MAX(DATEDIFF(ingestion_time, event_time)) as max_delay_days,
                COUNT(DISTINCT product_id) as affected_products,
                COUNT(DISTINCT store_id) as affected_stores
            FROM local.bronze.inventory_updates
            WHERE DATE(ingestion_time) = '{process_date}'
            AND is_late_arrival = true
            
            UNION ALL
            
            SELECT 
                'feedback' as data_type,
                COUNT(*) as record_count,
                AVG(DATEDIFF(ingestion_time, feedback_time)) as avg_delay_days,
                MAX(DATEDIFF(ingestion_time, feedback_time)) as max_delay_days,
                COUNT(DISTINCT product_id) as affected_products,
                0 as affected_stores
            FROM local.bronze.customer_feedback
            WHERE DATE(ingestion_time) = '{process_date}'
            AND DATEDIFF(ingestion_time, feedback_time) > 0
        )
        SELECT 
            data_type,
            record_count,
            ROUND(avg_delay_days, 2) as avg_delay_days,
            max_delay_days,
            affected_products,
            affected_stores,
            current_timestamp() as report_timestamp
        FROM late_arrivals
        WHERE record_count > 0
    """)
    
    # Save report
    report.write \
        .format("iceberg") \
        .mode("append") \
        .save("local.gold.late_arrival_reports")
    
    # Display report
    report.show()

def main():
    if len(sys.argv) != 3:
        print("Usage: late_arrival_handler.py <process_date> <window_hours>")
        sys.exit(1)
    
    process_date = sys.argv[1]
    window_hours = sys.argv[2]
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # Handle different types of late arrivals
        inventory_count = handle_late_inventory_data(spark, process_date, window_hours)
        feedback_count = handle_late_feedback_data(spark, process_date, window_hours)
        
        print(f"Processed {inventory_count} late inventory records")
        print(f"Processed {feedback_count} late feedback records")
        
        # Generate report
        if inventory_count > 0 or feedback_count > 0:
            generate_late_arrival_report(spark, process_date)
            
    except Exception as e:
        print(f"Error handling late arrivals: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()