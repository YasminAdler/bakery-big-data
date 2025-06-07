from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys
from datetime import datetime

def create_spark_session():
    """Create Spark session with Iceberg configuration"""
    return SparkSession.builder \
        .appName("SilverToGoldETL") \
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

def update_product_pricing_scd2(spark, process_date):
    """Update product pricing dimension with SCD Type 2"""
    
    # Read current dimension
    try:
        current_dim = spark.read \
            .format("iceberg") \
            .load("local.gold.dim_product_pricing") \
            .filter(col("is_current_record") == True)
    except:
        # Create empty dataframe if table doesn't exist
        schema = spark.createDataFrame([], 
            """pricing_key INT, product_id INT, product_name STRING, 
               base_price DECIMAL(10,2), price_category STRING, 
               marketing_strategy STRING, valid_from DATE, valid_to DATE, 
               is_current_record BOOLEAN, margin_percentage DECIMAL(5,2), 
               pricing_tier STRING""")
        current_dim = schema
    
    # Get latest product prices from silver
    latest_prices = spark.sql("""
        SELECT DISTINCT
            p.product_id,
            p.product_name,
            FIRST_VALUE(s.unit_price) OVER (
                PARTITION BY p.product_id 
                ORDER BY s.sale_date DESC, s.sale_time DESC
            ) as base_price
        FROM local.silver.sales s
        JOIN local.silver.products p ON s.product_id = p.product_id
        WHERE s.sale_date = '{}'
    """.format(process_date))
    
    # Add derived columns
    latest_prices = latest_prices.withColumn(
        "price_category",
        when(col("base_price") < 5, "Budget")
        .when(col("base_price") < 15, "Standard")
        .when(col("base_price") < 30, "Premium")
        .otherwise("Luxury")
    ).withColumn(
        "marketing_strategy",
        when(col("price_category") == "Budget", "Volume Sales")
        .when(col("price_category") == "Standard", "Value Proposition")
        .when(col("price_category") == "Premium", "Quality Focus")
        .otherwise("Exclusive Experience")
    ).withColumn(
        "margin_percentage",
        when(col("price_category") == "Budget", 20)
        .when(col("price_category") == "Standard", 35)
        .when(col("price_category") == "Premium", 45)
        .otherwise(60)
    ).withColumn(
        "pricing_tier",
        when(col("base_price") < 3, "Tier 1")
        .when(col("base_price") < 10, "Tier 2")
        .when(col("base_price") < 25, "Tier 3")
        .otherwise("Tier 4")
    )
    
    # Identify changed records
    changes = latest_prices.join(
        current_dim.select("product_id", "base_price"),
        "product_id",
        "left_anti"
    ).union(
        latest_prices.join(
            current_dim.select("product_id", "base_price"),
            "product_id",
            "inner"
        ).filter(col("latest_prices.base_price") != col("current_dim.base_price"))
    )
    
    if changes.count() > 0:
        # Generate new keys
        max_key = current_dim.agg(max("pricing_key")).collect()[0][0] or 0
        
        # Close old records
        updates = current_dim.join(
            changes.select("product_id"),
            "product_id",
            "inner"
        ).withColumn("is_current_record", lit(False)) \
         .withColumn("valid_to", lit(process_date))
        
        # Create new records
        new_records = changes.withColumn(
            "pricing_key", 
            row_number().over(Window.orderBy("product_id")) + max_key
        ).withColumn("valid_from", lit(process_date)) \
         .withColumn("valid_to", lit(None).cast("date")) \
         .withColumn("is_current_record", lit(True))
        
        # Combine all records
        final_dim = current_dim.join(
            changes.select("product_id"),
            "product_id",
            "left_anti"
        ).union(updates).union(new_records)
        
        # Write to gold layer
        final_dim.write \
            .format("iceberg") \
            .mode("overwrite") \
            .save("local.gold.dim_product_pricing")

def update_customer_scd2(spark, process_date):
    """Update customer dimension with SCD Type 2"""
    
    # Read current dimension
    try:
        current_dim = spark.read \
            .format("iceberg") \
            .load("local.gold.dim_customer") \
            .filter(col("is_current_record") == True)
    except:
        # Create empty dataframe if table doesn't exist
        schema = spark.createDataFrame([], 
            """customer_key INT, customer_id STRING, customer_name STRING,
               email STRING, phone STRING, loyalty_tier STRING,
               lifetime_value DECIMAL(10,2), first_purchase_date DATE,
               valid_from DATE, valid_to DATE, is_current_record BOOLEAN""")
        current_dim = schema
    
    # Get customer updates from silver
    customer_updates = spark.sql("""
        WITH customer_metrics AS (
            SELECT 
                customer_id,
                MIN(sale_date) as first_purchase_date,
                SUM(total_revenue) as lifetime_value,
                COUNT(DISTINCT sale_id) as purchase_count,
                AVG(total_revenue) as avg_purchase_value
            FROM local.silver.sales
            WHERE sale_date <= '{}'
            GROUP BY customer_id
        )
        SELECT 
            cm.*,
            CASE 
                WHEN lifetime_value > 1000 OR purchase_count > 50 THEN 'Platinum'
                WHEN lifetime_value > 500 OR purchase_count > 20 THEN 'Gold'
                WHEN lifetime_value > 200 OR purchase_count > 10 THEN 'Silver'
                ELSE 'Bronze'
            END as loyalty_tier
        FROM customer_metrics cm
    """.format(process_date))
    
    # Identify tier changes
    tier_changes = customer_updates.alias("new").join(
        current_dim.alias("curr"),
        col("new.customer_id") == col("curr.customer_id"),
        "inner"
    ).filter(col("new.loyalty_tier") != col("curr.loyalty_tier")) \
     .select("new.*")
    
    if tier_changes.count() > 0:
        # Process SCD Type 2 updates similar to product pricing
        # ... (implementation similar to product pricing SCD2)
        pass

def create_fact_sales(spark, process_date):
    """Create fact sales table"""
    
    sales_df = spark.sql("""
        SELECT 
            s.sale_id,
            s.product_id,
            s.store_id,
            c.customer_key,
            pp.pricing_key,
            s.sale_date as date,
            s.weather_id,
            s.marketing_event_id,
            s.quantity_sold,
            s.total_revenue,
            c.loyalty_tier as customer_loyalty_tier,
            s.time_of_day,
            CASE WHEN s.discount_percentage > 0 THEN true ELSE false END as promotion_applied,
            s.discount_amount,
            s.profit_margin,
            s.cost_of_goods_sold,
            ROW_NUMBER() OVER (
                PARTITION BY s.store_id, s.sale_date 
                ORDER BY s.sale_time
            ) as transaction_sequence
        FROM local.silver.sales s
        LEFT JOIN local.gold.dim_customer c 
            ON s.customer_id = c.customer_id 
            AND c.is_current_record = true
        LEFT JOIN local.gold.dim_product_pricing pp 
            ON s.product_id = pp.product_id 
            AND pp.is_current_record = true
        WHERE s.sale_date = '{}'
    """.format(process_date))
    
    # Write to fact table
    sales_df.write \
        .format("iceberg") \
        .mode("append") \
        .partitionBy("date") \
        .save("local.gold.fact_sales")

def create_ml_features(spark, process_date):
    """Create ML feature tables"""
    
    # Demand forecast features
    demand_features = spark.sql("""
        WITH sales_aggregates AS (
            SELECT 
                product_id,
                store_id,
                sale_date,
                SUM(quantity_sold) as daily_sales,
                AVG(quantity_sold) OVER (
                    PARTITION BY product_id, store_id 
                    ORDER BY sale_date 
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as sales_last_7_days,
                AVG(quantity_sold) OVER (
                    PARTITION BY product_id, store_id 
                    ORDER BY sale_date 
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) as sales_last_30_days,
                STDDEV(quantity_sold) OVER (
                    PARTITION BY product_id, store_id 
                    ORDER BY sale_date 
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) as sales_volatility
            FROM local.silver.sales
            WHERE sale_date BETWEEN DATE_SUB('{}', 30) AND '{}'
            GROUP BY product_id, store_id, sale_date
        ),
        inventory_data AS (
            SELECT 
                product_id,
                store_id,
                inventory_date,
                closing_stock as inventory_level,
                waste_ratio
            FROM local.silver.inventory
            WHERE inventory_date = '{}'
        )
        SELECT 
            CONCAT(sa.product_id, '-', sa.store_id, '-', sa.sale_date) as feature_id,
            sa.product_id,
            sa.store_id,
            sa.sale_date as date,
            sa.daily_sales as sales_volume,
            sa.daily_sales as avg_daily_sales,
            sa.sales_last_7_days,
            sa.sales_last_30_days,
            CASE 
                WHEN sa.sales_last_7_days > sa.sales_last_30_days * 1.2 THEN 'Increasing'
                WHEN sa.sales_last_7_days < sa.sales_last_30_days * 0.8 THEN 'Decreasing'
                ELSE 'Stable'
            END as sales_trend,
            -- Add seasonality index based on historical patterns
            sa.sales_volatility / NULLIF(sa.sales_last_30_days, 0) as seasonality_index,
            date_format(sa.sale_date, 'EEEE') as day_of_week,
            cal.is_holiday,
            cal.holiday_name,
            cal.season,
            COALESCE(p.is_active, false) as promotion_active,
            p.campaign_name as marketing_campaign,
            id.inventory_level,
            id.waste_ratio,
            pp.base_price as price,
            w.weather_impact_score,
            -- Placeholder for forecast accuracy (would be calculated after predictions)
            NULL as forecast_accuracy
        FROM sales_aggregates sa
        LEFT JOIN local.gold.dim_calendar cal ON sa.sale_date = cal.date
        LEFT JOIN local.gold.dim_product_pricing pp 
            ON sa.product_id = pp.product_id AND pp.is_current_record = true
        LEFT JOIN inventory_data id 
            ON sa.product_id = id.product_id 
            AND sa.store_id = id.store_id
        LEFT JOIN local.gold.active_promotions p 
            ON sa.product_id = p.product_id 
            AND sa.sale_date BETWEEN p.start_date AND p.end_date
        LEFT JOIN local.gold.weather_impact w 
            ON sa.store_id = w.store_id 
            AND sa.sale_date = w.date
        WHERE sa.sale_date = '{}'
    """.format(process_date, process_date, process_date, process_date))
    
    # Write ML features
    demand_features.write \
        .format("iceberg") \
        .mode("append") \
        .save("local.gold.fact_demand_forecast_features")
    
    # Equipment maintenance features
    equipment_features = spark.sql("""
        WITH equipment_history AS (
            SELECT 
                equipment_id,
                metric_date,
                AVG(temperature) as avg_temperature,
                MAX(temperature) - MIN(temperature) as temperature_fluctuation,
                SUM(power_consumption) as total_power_consumption,
                SUM(CASE WHEN maintenance_alert = true THEN 1 ELSE 0 END) as alert_count,
                SUM(operational_hours) as daily_operational_hours
            FROM local.silver.equipment_metrics
            WHERE metric_date BETWEEN DATE_SUB('{}', 30) AND '{}'
            GROUP BY equipment_id, metric_date
        ),
        maintenance_history AS (
            SELECT 
                equipment_id,
                MAX(maintenance_date) as last_maintenance_date,
                COUNT(*) as maintenance_count,
                AVG(maintenance_duration_hours) as avg_maintenance_duration
            FROM local.gold.maintenance_records
            WHERE maintenance_date < '{}'
            GROUP BY equipment_id
        )
        SELECT 
            CONCAT(eh.equipment_id, '-', eh.metric_date) as feature_id,
            eh.equipment_id,
            eh.metric_date as date,
            DATEDIFF(eh.metric_date, mh.last_maintenance_date) as days_since_last_maintenance,
            SUM(eh.daily_operational_hours) OVER (
                PARTITION BY eh.equipment_id 
                ORDER BY eh.metric_date 
                ROWS UNBOUNDED PRECEDING
            ) as total_operational_hours,
            eh.avg_temperature as avg_daily_temperature,
            eh.temperature_fluctuation,
            AVG(eh.total_power_consumption) OVER (
                PARTITION BY eh.equipment_id 
                ORDER BY eh.metric_date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as power_consumption_trend,
            SUM(eh.alert_count) OVER (
                PARTITION BY eh.equipment_id 
                ORDER BY eh.metric_date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as abnormal_events_count,
            DATEDIFF(eh.metric_date, e.purchase_date) as age_in_days,
            eh.daily_operational_hours / 24.0 as usage_intensity_score,
            mh.maintenance_count / NULLIF(DATEDIFF(eh.metric_date, e.purchase_date), 0) * 365 
                as maintenance_history_score,
            -- Simplified failure probability calculation
            CASE 
                WHEN days_since_last_maintenance > 90 THEN 0.8
                WHEN temperature_fluctuation > 50 THEN 0.6
                WHEN alert_count > 5 THEN 0.7
                ELSE 0.1
            END as failure_probability
        FROM equipment_history eh
        LEFT JOIN local.gold.dim_equipment e ON eh.equipment_id = e.equipment_id
        LEFT JOIN maintenance_history mh ON eh.equipment_id = mh.equipment_id
        WHERE eh.metric_date = '{}'
    """.format(process_date, process_date, process_date, process_date))
    
    # Write equipment features
    equipment_features.write \
        .format("iceberg") \
        .mode("append") \
        .save("local.gold.fact_equipment_maintenance_features")

def main():
    if len(sys.argv) != 3:
        print("Usage: silver_to_gold.py <process_type> <process_date>")
        sys.exit(1)
    
    process_type = sys.argv[1]
    process_date = sys.argv[2]
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        if process_type == "dimensions":
            update_product_pricing_scd2(spark, process_date)
            update_customer_scd2(spark, process_date)
            print(f"Updated dimension tables for {process_date}")
            
        elif process_type == "facts":
            create_fact_sales(spark, process_date)
            print(f"Created fact tables for {process_date}")
            
        elif process_type == "ml_features":
            create_ml_features(spark, process_date)
            print(f"Created ML feature tables for {process_date}")
            
        else:
            print(f"Unknown process type: {process_type}")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error in {process_type} processing: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()