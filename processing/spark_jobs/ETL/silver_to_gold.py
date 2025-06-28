"""
Silver to Gold ETL Job
Transforms standardized data from silver layer to business-ready gold layer with dimensions and facts.
"""

import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Iceberg support"""
    return SparkSession.builder \
        .appName("SilverToGoldETL") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://bakery-warehouse/gold") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def populate_dimensions(spark):
    """Populate dimension tables with reference data"""
    logger.info("📊 Populating dimension tables...")
    
    # Populate Equipment Dimension
    equipment_data = [
        (1, "Commercial Oven A", "Oven", "BakeryTech", "BT-OVN-001", "2023-01-15", 15.5, "Monthly", 10, 25000.00, True),
        (2, "Mixer Pro", "Mixer", "DoughMaster", "DM-MIX-002", "2023-02-20", 8.2, "Quarterly", 8, 12000.00, True),
        (3, "Refrigeration Unit", "Refrigerator", "ColdTech", "CT-REF-003", "2023-03-10", 12.0, "Semi-annually", 12, 18000.00, True),
        (4, "Display Case", "Display", "ShowCase", "SC-DIS-004", "2023-01-30", 3.5, "Annually", 15, 8000.00, True),
        (5, "Coffee Machine", "Beverage", "BrewMaster", "BM-COF-005", "2023-04-05", 5.8, "Monthly", 6, 15000.00, True)
    ]
    
    equipment_df = spark.createDataFrame(equipment_data, [
        "equipment_id", "equipment_name", "equipment_type", "manufacturer", 
        "model_number", "purchase_date", "power_consumption_kw", 
        "maintenance_frequency", "expected_lifespan_years", "replacement_cost", "is_current"
    ])
    
    equipment_df.writeTo("local.gold.dim_equipment").overwrite()
    logger.info("✅ Equipment dimension populated")
    
    # Populate Store Dimension
    store_data = [
        (1, 1, "Downtown", "Downtown", "123 Main St", "New York", "NY", "USA", "John Smith", 7.0, 22.0, 150.0, "2023-01-01", None, True, 150.0),
        (2, 2, "Mall", "Mall", "456 Shopping Ave", "Los Angeles", "CA", "USA", "Sarah Johnson", 9.0, 21.0, 120.0, "2023-01-01", None, True, 120.0),
        (3, 3, "Airport", "Airport", "789 Terminal Rd", "Chicago", "IL", "USA", "Mike Davis", 5.0, 23.0, 80.0, "2023-01-01", None, True, 80.0),
        (4, 4, "Suburban", "Suburban", "321 Oak Dr", "Houston", "TX", "USA", "Lisa Wilson", 8.0, 20.0, 100.0, "2023-01-01", None, True, 100.0),
        (5, 5, "University", "University", "654 Campus Blvd", "Boston", "MA", "USA", "Tom Brown", 8.0, 21.0, 90.0, "2023-01-01", None, True, 90.0)
    ]
    
    store_df = spark.createDataFrame(store_data, [
        "store_key", "store_id", "location", "type", "address", "city", "region", 
        "country", "manager_name", "opening_hour", "closing_hour", "total_area_sqm",
        "effective_date", "end_date", "is_current_flag", "area_sqm"
    ])
    
    store_df.writeTo("local.gold.dim_store").overwrite()
    logger.info("✅ Store dimension populated")
    
    # Populate Product Dimension
    product_data = [
        (1, "Croissant", "Pastry", "Viennoiserie", "Flour, Butter, Yeast", "Gluten, Dairy", 24, False, "Calories: 231, Fat: 12g"),
        (2, "Baguette", "Bread", "Artisan", "Flour, Water, Salt, Yeast", "Gluten", 48, False, "Calories: 265, Fat: 1g"),
        (3, "Chocolate Cake", "Cake", "Celebration", "Flour, Sugar, Eggs, Chocolate", "Gluten, Eggs, Dairy", 72, False, "Calories: 350, Fat: 18g"),
        (4, "Apple Pie", "Pie", "Fruit", "Flour, Apples, Sugar, Cinnamon", "Gluten", 48, True, "Calories: 280, Fat: 12g"),
        (5, "Sourdough Bread", "Bread", "Artisan", "Flour, Water, Salt, Sourdough Starter", "Gluten", 96, False, "Calories: 245, Fat: 1g"),
        (6, "Blueberry Muffin", "Muffin", "Quick Bread", "Flour, Sugar, Eggs, Blueberries", "Gluten, Eggs, Dairy", 48, False, "Calories: 320, Fat: 15g"),
        (7, "Cinnamon Roll", "Pastry", "Sweet", "Flour, Sugar, Cinnamon, Icing", "Gluten, Dairy", 24, False, "Calories: 380, Fat: 20g"),
        (8, "Cheesecake", "Cake", "Cream", "Cream Cheese, Sugar, Eggs, Graham Cracker", "Gluten, Eggs, Dairy", 96, False, "Calories: 400, Fat: 25g")
    ]
    
    product_df = spark.createDataFrame(product_data, [
        "product_id", "product_name", "category", "subcategory", "ingredients", 
        "allergens", "shelf_life_hours", "is_seasonal", "nutrition_info"
    ])
    
    product_df.writeTo("local.gold.dim_product").overwrite()
    logger.info("✅ Product dimension populated")
    
    # Populate Calendar Dimension (for the next year)
    calendar_data = []
    start_date = datetime(2024, 1, 1)
    for i in range(365):
        date = start_date + timedelta(days=i)
        calendar_data.append((
            date.date(),
            date.strftime("%A"),
            date.day,
            date.month,
            date.strftime("%B"),
            (date.month - 1) // 3 + 1,
            date.year,
            date.weekday() >= 5,
            False,  # Simplified - no holiday logic
            None,
            "Spring" if date.month in [3, 4, 5] else "Summer" if date.month in [6, 7, 8] else "Fall" if date.month in [9, 10, 11] else "Winter"
        ))
    
    calendar_df = spark.createDataFrame(calendar_data, [
        "date", "day_of_week", "day_of_month", "month", "month_name", 
        "quarter", "year", "is_weekend", "is_holiday", "holiday_name", "season"
    ])
    
    calendar_df.writeTo("local.gold.dim_calendar").overwrite()
    logger.info("✅ Calendar dimension populated")

def create_fact_sales(spark):
    """Create fact_sales table from silver_sales"""
    logger.info("🛒 Creating fact_sales table...")
    
    # Read from silver layer
    silver_sales = spark.read.format("iceberg").load("local.db.silver_sales")
    
    # Join with dimensions to create fact table
    fact_sales = silver_sales.select(
        col("sale_id"),
        col("product_id"),
        col("store_id"),
        lit(None).cast("int").alias("customer_key"),  # Will be enriched when customer dim is available
        lit(None).cast("int").alias("pricing_key"),   # Will be enriched when pricing dim is available
        col("sale_date").alias("date"),
        col("weather_id"),
        lit(None).cast("string").alias("marketing_event_id"),
        col("quantity_sold"),
        col("total_revenue"),
        lit("Regular").alias("customer_loyalty_tier"),  # Default value
        col("time_of_day"),
        when(col("promo_id").isNotNull(), True).otherwise(False).alias("promotion_applied"),
        lit(0.0).cast("decimal(10,2)").alias("discount_amount"),  # Will be calculated
        lit(0.25).cast("decimal(5,2)").alias("profit_margin"),    # Default 25% margin
        (col("total_revenue") * 0.75).alias("cost_of_goods_sold"), # 75% of revenue as cost
        monotonically_increasing_id().alias("transaction_sequence")
    )
    
    # Write to gold layer
    fact_sales.writeTo("local.gold.fact_sales").overwritePartitions()
    
    logger.info(f"✅ Fact sales created with {fact_sales.count()} records")
    return fact_sales.count()

def create_fact_inventory(spark):
    """Create fact_inventory table from silver_inventory"""
    logger.info("📦 Creating fact_inventory table...")
    
    # Read from silver layer
    silver_inventory = spark.read.format("iceberg").load("local.db.silver_inventory")
    
    # Create fact table
    fact_inventory = silver_inventory.select(
        col("inventory_id"),
        col("product_id"),
        col("store_id"),
        col("inventory_date").alias("date"),
        col("beginning_stock"),
        col("restocked_quantity"),
        col("sold_quantity"),
        col("waste_quantity"),
        col("waste_ratio"),
        col("days_of_supply"),
        when(col("closing_stock") == 0, 1).otherwise(0).alias("stock_out_events")
    )
    
    # Write to gold layer
    fact_inventory.writeTo("local.gold.fact_inventory").overwritePartitions()
    
    logger.info(f"✅ Fact inventory created with {fact_inventory.count()} records")
    return fact_inventory.count()

def create_fact_customer_feedback(spark):
    """Create fact_customer_feedback table from silver_customer_feedback"""
    logger.info("💬 Creating fact_customer_feedback table...")
    
    # Read from silver layer
    silver_feedback = spark.read.format("iceberg").load("local.db.silver_customer_feedback")
    
    # Create fact table
    fact_feedback = silver_feedback.select(
        col("feedback_id"),
        lit(None).cast("int").alias("customer_key"),  # Will be enriched when customer dim is available
        col("product_id"),
        col("platform"),
        col("rating"),
        col("review_text"),
        col("feedback_date")
    )
    
    # Write to gold layer
    fact_feedback.writeTo("local.gold.fact_customer_feedback").overwritePartitions()
    
    logger.info(f"✅ Fact customer feedback created with {fact_feedback.count()} records")
    return fact_feedback.count()

def create_fact_equipment_performance(spark):
    """Create fact_equipment_performance table from silver_equipment_metrics"""
    logger.info("⚙️ Creating fact_equipment_performance table...")
    
    # Read from silver layer
    silver_equipment = spark.read.format("iceberg").load("local.db.silver_equipment_metrics")
    
    # Aggregate daily metrics
    fact_equipment = silver_equipment.groupBy("equipment_id", "metric_date").agg(
        sum("operational_hours").alias("operational_hours"),
        avg("power_consumption").alias("avg_temperature"),  # Reusing field for temperature
        sum("power_consumption").alias("total_power_consumption"),
        sum(when(col("maintenance_alert"), 1).otherwise(0)).alias("maintenance_events"),
        lit(0).alias("downtime_minutes"),  # Will be calculated from operational status
        lit(0.85).cast("decimal(5,2)").alias("efficiency_score")  # Default efficiency
    ).select(
        concat(lit("perf_"), col("equipment_id"), lit("_"), date_format(col("metric_date"), "yyyyMMdd")).alias("performance_id"),
        col("equipment_id"),
        col("metric_date").alias("date"),
        col("operational_hours"),
        col("avg_temperature"),
        col("total_power_consumption"),
        col("maintenance_events"),
        col("downtime_minutes"),
        col("efficiency_score")
    )
    
    # Write to gold layer
    fact_equipment.writeTo("local.gold.fact_equipment_performance").overwritePartitions()
    
    logger.info(f"✅ Fact equipment performance created with {fact_equipment.count()} records")
    return fact_equipment.count()

def main():
    """Main ETL process"""
    logger.info("🚀 Starting Silver to Gold ETL process...")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Populate dimensions first
        populate_dimensions(spark)
        
        # Create fact tables
        sales_count = create_fact_sales(spark)
        inventory_count = create_fact_inventory(spark)
        feedback_count = create_fact_customer_feedback(spark)
        equipment_count = create_fact_equipment_performance(spark)
        
        # Log summary
        logger.info("📊 Gold Layer ETL Summary:")
        logger.info(f"   - Fact sales records: {sales_count}")
        logger.info(f"   - Fact inventory records: {inventory_count}")
        logger.info(f"   - Fact feedback records: {feedback_count}")
        logger.info(f"   - Fact equipment records: {equipment_count}")
        logger.info(f"   - Total fact records: {sales_count + inventory_count + feedback_count + equipment_count}")
        
        logger.info("✅ Silver to Gold ETL completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ ETL process failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()