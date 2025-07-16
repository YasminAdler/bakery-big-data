#!/usr/bin/env python3
"""
Silver to Gold ETL Job
Transforms standardized data from Silver to business-ready Gold layer
Implements SCD Type 2 for Store and Product Pricing dimensions
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create Spark session with Iceberg configuration"""
    return SparkSession.builder \
        .appName("Silver to Gold ETL") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://iceberg-warehouse/") \
        .getOrCreate()


def load_or_create_dim_calendar(spark):
    """Create or update calendar dimension"""
    logger.info("Loading calendar dimension...")
    
    # Check if calendar already exists
    try:
        existing_calendar = spark.sql("SELECT MAX(date) as max_date FROM local.gold.dim_calendar")
        start_date = existing_calendar.first()["max_date"] + timedelta(days=1)
    except:
        start_date = datetime(2024, 1, 1).date()
    
    end_date = datetime.now().date() + timedelta(days=365)
    
    if start_date < end_date:
        # Generate date range
        date_df = spark.sql(f"""
            SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as date
        """)
        
        # Enrich with calendar attributes
        calendar_df = date_df.select(
            col("date"),
            date_format("date", "EEEE").alias("day_of_week"),
            dayofmonth("date").alias("day_of_month"),
            month("date").alias("month"),
            date_format("date", "MMMM").alias("month_name"),
            quarter("date").alias("quarter"),
            year("date").alias("year"),
            when(date_format("date", "E").isin("Sat", "Sun"), True).otherwise(False).alias("is_weekend"),
            lit(False).alias("is_holiday"),  # Simplified - would integrate with holiday API
            lit(None).alias("holiday_name"),
            when(month("date").between(3, 5), "Spring")
                .when(month("date").between(6, 8), "Summer")
                .when(month("date").between(9, 11), "Fall")
                .otherwise("Winter").alias("season")
        )
        
        calendar_df.write.mode("append").saveAsTable("local.gold.dim_calendar")
        logger.info(f"Added {calendar_df.count()} dates to calendar dimension")


def load_dim_product(spark):
    """Load product dimension (static)"""
    logger.info("Loading product dimension...")
    
    products_data = [
        (1, "Croissant", "Pastry", "Viennoiserie", "Flour, Butter, Yeast, Salt, Sugar", "Gluten, Dairy", 24, False, "Calories: 250"),
        (2, "Baguette", "Bread", "French Bread", "Flour, Water, Yeast, Salt", "Gluten", 48, False, "Calories: 180"),
        (3, "Pain au Chocolat", "Pastry", "Viennoiserie", "Flour, Butter, Chocolate, Yeast", "Gluten, Dairy", 24, False, "Calories: 300"),
        (4, "Sourdough Loaf", "Bread", "Artisan Bread", "Flour, Water, Salt, Starter", "Gluten", 72, False, "Calories: 200"),
        (5, "Blueberry Muffin", "Pastry", "Muffin", "Flour, Sugar, Blueberries, Eggs", "Gluten, Eggs", 48, True, "Calories: 280"),
        (6, "Whole Wheat Roll", "Bread", "Healthy Bread", "Whole Wheat Flour, Water, Yeast", "Gluten", 36, False, "Calories: 150"),
        (7, "Apple Tart", "Pastry", "Tart", "Flour, Butter, Apples, Sugar", "Gluten, Dairy", 24, True, "Calories: 320"),
        (8, "Rye Bread", "Bread", "Artisan Bread", "Rye Flour, Water, Yeast, Caraway", "Gluten", 60, False, "Calories: 190"),
        (9, "Chocolate Eclair", "Pastry", "French Pastry", "Choux Pastry, Chocolate, Cream", "Gluten, Dairy, Eggs", 12, False, "Calories: 350"),
        (10, "French Bread", "Bread", "Classic Bread", "Flour, Water, Yeast, Salt", "Gluten", 36, False, "Calories: 170")
    ]
    
    columns = ["product_id", "product_name", "category", "subcategory", "ingredients", "allergens", "shelf_life_hours", "is_seasonal", "nutrition_info"]
    
    products_df = spark.createDataFrame(products_data, columns)
    
    # Merge to avoid duplicates
    products_df.createOrReplaceTempView("new_products")
    
    spark.sql("""
        MERGE INTO local.gold.dim_product t
        USING new_products s
        ON t.product_id = s.product_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    logger.info("Product dimension loaded")


def update_dim_store_scd2(spark, process_date):
    """Update store dimension with SCD Type 2 logic"""
    logger.info("Updating store dimension (SCD Type 2)...")
    
    # Simulated store changes (in production, would come from source system)
    current_stores_data = [
        (1, "Downtown", "Regular", "123 Main St", "New York", "NY", "USA", "John Smith", 6.0, 20.0, 500.0),
        (2, "Mall", "Kiosk", "456 Mall Ave", "New York", "NY", "USA", "Emma Johnson", 8.0, 22.0, 300.0),
        (3, "Airport", "Express", "789 Airport Rd", "New York", "NY", "USA", "Michael Brown", 5.0, 23.0, 400.0),
        (4, "Suburb North", "Regular", "321 North Blvd", "New York", "NY", "USA", "Sarah Davis", 7.0, 19.0, 450.0),
        (5, "Suburb South", "Regular", "654 South St", "New York", "NY", "USA", "David Wilson", 7.0, 19.0, 450.0)
    ]
    
    columns = ["store_id", "location", "type", "address", "city", "region", "country", 
               "manager_name", "opening_hour", "closing_hour", "total_area_sqm"]
    
    current_stores_df = spark.createDataFrame(current_stores_data, columns) \
        .withColumn("area_sqm", col("total_area_sqm"))
    
    # Get existing current records
    existing_stores = spark.sql("""
        SELECT * FROM local.gold.dim_store
        WHERE is_current_flag = true
    """)
    
    # Detect changes
    changes = current_stores_df.alias("new").join(
        existing_stores.alias("old"),
        col("new.store_id") == col("old.store_id"),
        "left"
    ).where(
        col("old.store_id").isNull() |  # New stores
        (col("new.manager_name") != col("old.manager_name")) |  # Manager changed
        (col("new.opening_hour") != col("old.opening_hour")) |  # Hours changed
        (col("new.closing_hour") != col("old.closing_hour")) |
        (col("new.total_area_sqm") != col("old.total_area_sqm"))  # Area changed
    )
    
    if changes.count() > 0:
        # Close existing records for changed stores
        changed_store_ids = changes.select("store_id").rdd.flatMap(lambda x: x).collect()
        
        spark.sql(f"""
            UPDATE local.gold.dim_store
            SET is_current_flag = false,
                end_date = '{process_date}'
            WHERE store_id IN ({','.join(map(str, changed_store_ids))})
            AND is_current_flag = true
        """)
        
        # Insert new records
        max_key = spark.sql("SELECT COALESCE(MAX(store_key), 0) as max_key FROM local.gold.dim_store").first()["max_key"]
        
        new_records = changes.select("new.*") \
            .withColumn("store_key", monotonically_increasing_id() + max_key + 1) \
            .withColumn("effective_date", lit(process_date)) \
            .withColumn("end_date", lit(None).cast("date")) \
            .withColumn("is_current_flag", lit(True))
        
        new_records.write.mode("append").saveAsTable("local.gold.dim_store")
        logger.info(f"Updated {new_records.count()} store records")


def update_dim_product_pricing_scd2(spark, process_date):
    """Update product pricing dimension with SCD Type 2 logic"""
    logger.info("Updating product pricing dimension (SCD Type 2)...")
    
    # Calculate average prices from recent sales
    recent_prices = spark.sql(f"""
        SELECT 
            product_id,
            ROUND(AVG(unit_price), 2) as base_price,
            CASE 
                WHEN AVG(unit_price) < 3 THEN 'Budget'
                WHEN AVG(unit_price) < 4 THEN 'Standard'
                ELSE 'Premium'
            END as price_category
        FROM local.silver.sales
        WHERE sale_date >= date_sub('{process_date}', 7)
        GROUP BY product_id
    """)
    
    # Get product names
    products = spark.sql("SELECT product_id, product_name FROM local.gold.dim_product")
    
    current_pricing = recent_prices.join(products, "product_id") \
        .withColumn("marketing_strategy", 
            when(col("price_category") == "Budget", "Volume Sales")
            .when(col("price_category") == "Standard", "Balanced")
            .otherwise("Premium Quality")) \
        .withColumn("base_price_creation_date", lit(process_date))
    
    # Get existing current pricing
    existing_pricing = spark.sql("""
        SELECT * FROM local.gold.dim_product_pricing
        WHERE is_current_record = true
    """)
    
    # Detect price changes (threshold: 5%)
    price_changes = current_pricing.alias("new").join(
        existing_pricing.alias("old"),
        col("new.product_id") == col("old.product_id"),
        "left"
    ).where(
        col("old.product_id").isNull() |  # New products
        (abs(col("new.base_price") - col("old.base_price")) / col("old.base_price") > 0.05)  # 5% change
    )
    
    if price_changes.count() > 0:
        # Close existing records
        changed_product_ids = price_changes.select("product_id").rdd.flatMap(lambda x: x).collect()
        
        spark.sql(f"""
            UPDATE local.gold.dim_product_pricing
            SET is_current_record = false,
                end_date = '{process_date}'
            WHERE product_id IN ({','.join(map(str, changed_product_ids))})
            AND is_current_record = true
        """)
        
        # Insert new records
        max_key = spark.sql("SELECT COALESCE(MAX(pricing_key), 0) as max_key FROM local.gold.dim_product_pricing").first()["max_key"]
        
        new_pricing = price_changes.select("new.*") \
            .withColumn("pricing_key", monotonically_increasing_id() + max_key + 1) \
            .withColumn("effective_date", lit(process_date)) \
            .withColumn("end_date", lit(None).cast("date")) \
            .withColumn("is_current_record", lit(True))
        
        new_pricing.write.mode("append").saveAsTable("local.gold.dim_product_pricing")
        logger.info(f"Updated {new_pricing.count()} pricing records")


def update_dim_customer(spark, process_date):
    """Update customer dimension"""
    logger.info("Updating customer dimension...")
    
    # Get unique customers from recent sales
    recent_customers = spark.sql(f"""
        SELECT DISTINCT 
            customer_id,
            MAX(sale_date) as last_visit_date
        FROM local.silver.sales
        WHERE sale_date >= date_sub('{process_date}', 30)
        GROUP BY customer_id
    """)
    
    # Generate customer details (in production, would come from CRM)
    customer_details = recent_customers \
        .withColumn("customer_name", concat(lit("Customer "), col("customer_id"))) \
        .withColumn("email", concat(col("customer_id"), lit("@bakery.com"))) \
        .withColumn("phone", lit("555-0100")) \
        .withColumn("address", lit("123 Customer St")) \
        .withColumn("city", lit("New York")) \
        .withColumn("region", lit("NY")) \
        .withColumn("country", lit("USA")) \
        .withColumn("joining_date", date_sub(col("last_visit_date"), 90)) \
        .withColumn("is_current_record", lit(True))
    
    # Generate customer keys
    max_key = spark.sql("SELECT COALESCE(MAX(customer_key), 0) as max_key FROM local.gold.dim_customer").first()["max_key"]
    
    new_customers = customer_details \
        .withColumn("customer_key", monotonically_increasing_id() + max_key + 1)
    
    # Merge new customers
    new_customers.createOrReplaceTempView("new_customers")
    
    spark.sql("""
        MERGE INTO local.gold.dim_customer t
        USING new_customers s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET
            last_visit_date = s.last_visit_date
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    logger.info("Customer dimension updated")


def load_fact_sales(spark, process_date):
    """Load sales fact table"""
    logger.info(f"Loading sales facts for date: {process_date}")
    
    # Get silver sales data
    silver_sales = spark.sql(f"""
        SELECT * FROM local.silver.sales
        WHERE sale_date = '{process_date}'
        AND data_quality_score >= 50  -- Only good quality data
    """)
    
    # Join with dimensions
    fact_sales = silver_sales \
        .join(spark.sql("SELECT customer_key, customer_id FROM local.gold.dim_customer"), "customer_id", "left") \
        .join(spark.sql("SELECT pricing_key, product_id FROM local.gold.dim_product_pricing WHERE is_current_record = true"), "product_id", "left") \
        .withColumn("date", col("sale_date")) \
        .withColumn("marketing_event_id", lit(None)) \
        .withColumn("customer_loyalty_tier", 
            when(col("customer_key").isNotNull(), "Regular").otherwise("New")) \
        .withColumn("promotion_applied", col("promo_id").isNotNull()) \
        .withColumn("discount_amount", 
            when(col("promo_id").isNotNull(), col("total_revenue") * 0.1).otherwise(0)) \
        .withColumn("cost_of_goods_sold", col("total_revenue") * 0.6) \
        .withColumn("profit_margin", 
            (col("total_revenue") - col("cost_of_goods_sold")) / col("total_revenue")) \
        .withColumn("transaction_sequence", row_number().over(Window.partitionBy("store_id", "date").orderBy("sale_time")))
    
    # Select final columns
    final_fact_sales = fact_sales.select(
        "sale_id", "product_id", "store_id", "customer_key", "pricing_key",
        "date", "weather_id", "marketing_event_id", "quantity_sold",
        "total_revenue", "customer_loyalty_tier", "time_of_day",
        "promotion_applied", "discount_amount", "profit_margin",
        "cost_of_goods_sold", "transaction_sequence"
    )
    
    # Write to fact table
    final_fact_sales.write.mode("append").saveAsTable("local.gold.fact_sales")
    
    record_count = final_fact_sales.count()
    logger.info(f"Loaded {record_count} sales facts")
    
    return record_count


def load_fact_inventory(spark, process_date):
    """Load inventory fact table"""
    logger.info(f"Loading inventory facts for date: {process_date}")
    
    # Get silver inventory data
    silver_inventory = spark.sql(f"""
        SELECT * FROM local.silver.inventory
        WHERE inventory_date = '{process_date}'
        AND data_quality_score >= 50
    """)
    
    # Calculate stock-out events
    fact_inventory = silver_inventory \
        .withColumn("date", col("inventory_date")) \
        .withColumn("stock_out_events", 
            when(col("closing_stock") == 0, 1).otherwise(0))
    
    # Select final columns
    final_fact_inventory = fact_inventory.select(
        "inventory_id", "product_id", "store_id", "date",
        "beginning_stock", "restocked_quantity", "sold_quantity",
        "waste_quantity", "waste_ratio", "days_of_supply", "stock_out_events"
    )
    
    # Write to fact table
    final_fact_inventory.write.mode("append").saveAsTable("local.gold.fact_inventory")
    
    record_count = final_fact_inventory.count()
    logger.info(f"Loaded {record_count} inventory facts")
    
    return record_count


def load_fact_equipment_performance(spark, process_date):
    """Load equipment performance fact table"""
    logger.info(f"Loading equipment performance facts for date: {process_date}")
    
    # Aggregate equipment metrics by day
    daily_performance = spark.sql(f"""
        SELECT 
            equipment_id,
            metric_date as date,
            SUM(operational_hours) as operational_hours,
            AVG(power_consumption) as avg_power_consumption,
            SUM(power_consumption * operational_hours) as total_power_consumption,
            SUM(CASE WHEN maintenance_alert = true THEN 1 ELSE 0 END) as maintenance_events,
            SUM(CASE WHEN operational_status IN ('error', 'maintenance') THEN 30 ELSE 0 END) as downtime_minutes
        FROM local.silver.equipment_metrics
        WHERE metric_date = '{process_date}'
        AND data_quality_score >= 50
        GROUP BY equipment_id, metric_date
    """)
    
    # Calculate efficiency score
    fact_performance = daily_performance \
        .withColumn("performance_id", concat(col("equipment_id"), lit("_"), col("date"))) \
        .withColumn("avg_temperature", lit(None).cast("decimal(5,2)")) \
        .withColumn("efficiency_score", 
            greatest(lit(0), least(lit(100), 
                (col("operational_hours") / 24 * 100) - (col("downtime_minutes") / 60 * 10)
            )))
    
    # Write to fact table
    fact_performance.write.mode("append").saveAsTable("local.gold.fact_equipment_performance")
    
    record_count = fact_performance.count()
    logger.info(f"Loaded {record_count} equipment performance facts")
    
    return record_count


def main():
    """Main execution function"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Get process date (default to yesterday)
    process_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    try:
        logger.info(f"Starting Silver to Gold ETL for date: {process_date}")
        
        # Update dimensions
        load_or_create_dim_calendar(spark)
        load_dim_product(spark)
        update_dim_store_scd2(spark, process_date)
        update_dim_product_pricing_scd2(spark, process_date)
        update_dim_customer(spark, process_date)
        
        # Load facts
        sales_count = load_fact_sales(spark, process_date)
        inventory_count = load_fact_inventory(spark, process_date)
        equipment_count = load_fact_equipment_performance(spark, process_date)
        
        logger.info(f"Silver to Gold ETL completed successfully!")
        logger.info(f"Facts loaded - Sales: {sales_count}, Inventory: {inventory_count}, Equipment: {equipment_count}")
        
    except Exception as e:
        logger.error(f"Error in Silver to Gold ETL: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main() 