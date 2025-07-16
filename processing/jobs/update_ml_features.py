#!/usr/bin/env python3
"""
Update ML Features Job
Generates and updates machine learning feature tables in the Gold layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark import SparkContext
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create Spark session with Iceberg configuration"""
    # Use existing session or create a new one
    # Iceberg configs should be passed via spark-submit parameters
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession(SparkContext.getOrCreate())
    return spark


def update_demand_forecast_features(spark, process_date):
    """Update demand forecasting ML features"""
    logger.info(f"Updating demand forecast features for date: {process_date}")
    
    # Calculate date ranges
    date_7_days_ago = (datetime.strptime(process_date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
    date_30_days_ago = (datetime.strptime(process_date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
    
    # Get sales data with calendar information
    sales_with_calendar = spark.sql(f"""
        SELECT 
            s.*,
            c.day_of_week,
            c.is_weekend,
            c.is_holiday,
            c.holiday_name,
            c.season
        FROM local.gold.fact_sales s
        JOIN local.gold.dim_calendar c ON s.date = c.date
        WHERE s.date BETWEEN '{date_30_days_ago}' AND '{process_date}'
    """)
    
    # Calculate sales aggregations by product and store
    sales_aggregations = spark.sql(f"""
        SELECT 
            product_id,
            store_id,
            SUM(CASE WHEN date = '{process_date}' THEN quantity_sold ELSE 0 END) as sales_volume,
            AVG(quantity_sold) as avg_daily_sales,
            SUM(CASE WHEN date >= '{date_7_days_ago}' THEN total_revenue ELSE 0 END) as sales_last_7_days,
            SUM(total_revenue) as sales_last_30_days
        FROM local.gold.fact_sales
        WHERE date BETWEEN '{date_30_days_ago}' AND '{process_date}'
        GROUP BY product_id, store_id
    """)
    
    # Calculate sales trend using window functions
    window_spec = Window.partitionBy("product_id", "store_id").orderBy("date").rowsBetween(-7, 0)
    
    sales_trend = sales_with_calendar \
        .groupBy("product_id", "store_id", "date") \
        .agg(sum("quantity_sold").alias("daily_sales")) \
        .withColumn("moving_avg", avg("daily_sales").over(window_spec)) \
        .withColumn("trend_direction", 
            when(col("daily_sales") > col("moving_avg"), "increasing")
            .when(col("daily_sales") < col("moving_avg"), "decreasing")
            .otherwise("stable")) \
        .where(col("date") == process_date) \
        .select("product_id", "store_id", "trend_direction")
    
    # Get inventory levels
    inventory_levels = spark.sql(f"""
        SELECT 
            product_id,
            store_id,
            closing_stock as inventory_level,
            waste_ratio
        FROM local.gold.fact_inventory
        WHERE date = '{process_date}'
    """)
    
    # Get current pricing
    current_pricing = spark.sql("""
        SELECT 
            product_id,
            base_price as price
        FROM local.gold.dim_product_pricing
        WHERE is_current_record = true
    """)
    
    # Check for active promotions
    active_promotions = spark.sql(f"""
        SELECT DISTINCT
            product_id,
            true as promotion_active,
            promo_type as marketing_campaign
        FROM local.gold.fact_promotions
        WHERE '{process_date}' BETWEEN start_date AND end_date
        AND is_active = true
    """)
    
    # Get calendar features for the process date
    calendar_features = spark.sql(f"""
        SELECT 
            day_of_week,
            is_holiday,
            holiday_name,
            season
        FROM local.gold.dim_calendar
        WHERE date = '{process_date}'
    """).first()
    
    # Calculate seasonality index (simplified - in production would use historical patterns)
    seasonality_index = spark.sql(f"""
        SELECT 
            product_id,
            AVG(CASE WHEN c.season = '{calendar_features['season']}' THEN s.quantity_sold ELSE NULL END) / 
            NULLIF(AVG(s.quantity_sold), 0) as seasonality_index
        FROM local.gold.fact_sales s
        JOIN local.gold.dim_calendar c ON s.date = c.date
        WHERE s.date BETWEEN DATE_SUB('{process_date}', 365) AND '{process_date}'
        GROUP BY product_id
    """)
    
    # Combine all features
    demand_features = sales_aggregations \
        .join(sales_trend, ["product_id", "store_id"], "left") \
        .join(inventory_levels, ["product_id", "store_id"], "left") \
        .join(current_pricing, ["product_id"], "left") \
        .join(active_promotions, ["product_id"], "left") \
        .join(seasonality_index, ["product_id"], "left") \
        .withColumn("feature_id", concat(col("product_id"), lit("_"), col("store_id"), lit("_"), lit(process_date))) \
        .withColumn("date", lit(process_date)) \
        .withColumn("sales_trend", col("trend_direction")) \
        .withColumn("day_of_week", lit(calendar_features['day_of_week'])) \
        .withColumn("is_holiday", lit(calendar_features['is_holiday'])) \
        .withColumn("holiday_name", lit(calendar_features['holiday_name'])) \
        .withColumn("season", lit(calendar_features['season'])) \
        .withColumn("promotion_active", coalesce(col("promotion_active"), lit(False))) \
        .withColumn("marketing_campaign", col("marketing_campaign")) \
        .withColumn("weather_impact_score", lit(0.0)) \
        .withColumn("forecast_accuracy", lit(None).cast("decimal(5,4)"))
    
    # Select final columns
    final_features = demand_features.select(
        "feature_id", "product_id", "store_id", "date",
        "sales_volume", "avg_daily_sales", "sales_last_7_days", "sales_last_30_days",
        "sales_trend", "seasonality_index", "day_of_week", "is_holiday",
        "holiday_name", "season", "promotion_active", "marketing_campaign",
        "inventory_level", "waste_ratio", "price", "weather_impact_score",
        "forecast_accuracy"
    )
    
    # Write to feature table
    final_features.write.mode("append").saveAsTable("local.gold.fact_demand_forecast_features")
    
    record_count = final_features.count()
    logger.info(f"Generated {record_count} demand forecast features")
    
    return record_count


def update_equipment_maintenance_features(spark, process_date):
    """Update equipment maintenance prediction features"""
    logger.info(f"Updating equipment maintenance features for date: {process_date}")
    
    # Get equipment performance history
    equipment_history = spark.sql(f"""
        SELECT 
            equipment_id,
            date,
            operational_hours,
            total_power_consumption,
            maintenance_events,
            downtime_minutes
        FROM local.gold.fact_equipment_performance
        WHERE date <= '{process_date}'
        ORDER BY equipment_id, date
    """)
    
    # Calculate days since last maintenance
    maintenance_window = Window.partitionBy("equipment_id").orderBy("date")
    
    last_maintenance = equipment_history \
        .where(col("maintenance_events") > 0) \
        .groupBy("equipment_id") \
        .agg(max("date").alias("last_maintenance_date"))
    
    # Calculate cumulative operational hours
    cumulative_hours = equipment_history \
        .withColumn("total_operational_hours", 
            sum("operational_hours").over(maintenance_window)) \
        .where(col("date") == process_date)
    
    # Calculate temperature statistics (from equipment metrics)
    temp_stats = spark.sql(f"""
        SELECT 
            equipment_id,
            AVG(power_consumption) as avg_daily_temperature,  -- Proxy for temperature
            STDDEV(power_consumption) as temperature_fluctuation
        FROM local.silver.equipment_metrics
        WHERE metric_date = '{process_date}'
        GROUP BY equipment_id
    """)
    
    # Calculate power consumption trend
    power_trend_window = Window.partitionBy("equipment_id").orderBy("date").rowsBetween(-7, 0)
    
    power_trend = equipment_history \
        .withColumn("avg_power_7_days", avg("total_power_consumption").over(power_trend_window)) \
        .where(col("date") == process_date) \
        .withColumn("power_consumption_trend", 
            col("total_power_consumption") - col("avg_power_7_days"))
    
    # Count abnormal events
    abnormal_events = spark.sql(f"""
        SELECT 
            equipment_id,
            COUNT(*) as abnormal_events_count
        FROM local.silver.equipment_metrics
        WHERE metric_date BETWEEN DATE_SUB('{process_date}', 7) AND '{process_date}'
        AND operational_status IN ('error', 'maintenance')
        GROUP BY equipment_id
    """)
    
    # Get equipment age
    equipment_info = spark.sql("""
        SELECT 
            equipment_id,
            DATEDIFF(CURRENT_DATE(), purchase_date) as age_in_days,
            expected_lifespan_years * 365 as expected_lifespan_days
        FROM local.gold.dim_equipment
        WHERE is_current = true
    """)
    
    # Combine all features
    maintenance_features = cumulative_hours \
        .join(last_maintenance, ["equipment_id"], "left") \
        .join(temp_stats, ["equipment_id"], "left") \
        .join(power_trend.select("equipment_id", "power_consumption_trend"), ["equipment_id"], "left") \
        .join(abnormal_events, ["equipment_id"], "left") \
        .join(equipment_info, ["equipment_id"], "left") \
        .withColumn("feature_id", concat(col("equipment_id"), lit("_"), lit(process_date))) \
        .withColumn("date", lit(process_date)) \
        .withColumn("days_since_last_maintenance", 
            coalesce(datediff(lit(process_date), col("last_maintenance_date")), col("age_in_days"))) \
        .withColumn("abnormal_events_count", coalesce(col("abnormal_events_count"), lit(0))) \
        .withColumn("usage_intensity_score", 
            col("total_operational_hours") / (col("age_in_days") * 24) * 100) \
        .withColumn("maintenance_history_score", 
            when(col("days_since_last_maintenance") > 90, 0.2)
            .when(col("days_since_last_maintenance") > 60, 0.5)
            .when(col("days_since_last_maintenance") > 30, 0.8)
            .otherwise(1.0)) \
        .withColumn("failure_probability", 
            least(
                lit(1.0),
                (col("days_since_last_maintenance") / 365.0) * 
                (col("abnormal_events_count") / 10.0) * 
                (col("age_in_days") / col("expected_lifespan_days"))
            ))
    
    # Select final columns
    final_features = maintenance_features.select(
        "feature_id", "equipment_id", "date",
        "days_since_last_maintenance", "total_operational_hours",
        "avg_daily_temperature", "temperature_fluctuation",
        "power_consumption_trend", "abnormal_events_count",
        "age_in_days", "usage_intensity_score",
        "maintenance_history_score", "failure_probability"
    )
    
    # Write to feature table
    final_features.write.mode("append").saveAsTable("local.gold.fact_equipment_maintenance_features")
    
    record_count = final_features.count()
    logger.info(f"Generated {record_count} equipment maintenance features")
    
    return record_count


def update_product_quality_features(spark, process_date):
    """Update product quality prediction features"""
    logger.info(f"Updating product quality features for date: {process_date}")
    
    # For demo purposes, generate synthetic quality features
    # In production, this would come from production systems
    
    products = spark.sql("SELECT product_id FROM local.gold.dim_product")
    equipment = spark.sql("SELECT equipment_id FROM local.gold.dim_equipment WHERE equipment_type = 'oven'")
    
    # Generate production batches for the day
    production_batches = products.crossJoin(equipment) \
        .withColumn("production_batch_id", concat(lit("BATCH_"), col("product_id"), lit("_"), col("equipment_id"), lit("_"), lit(process_date))) \
        .withColumn("feature_id", concat(col("product_id"), lit("_"), col("equipment_id"), lit("_"), lit(process_date))) \
        .withColumn("production_date", lit(process_date)) \
        .withColumn("baker_id", (rand() * 10).cast("int") + 1) \
        .withColumn("ingredient_quality_score", rand() * 20 + 80) \
        .withColumn("process_adherence_score", rand() * 15 + 85) \
        .withColumn("proofing_time_minutes", (rand() * 30 + 30).cast("int")) \
        .withColumn("baking_temperature", rand() * 50 + 175) \
        .withColumn("baking_time_minutes", (rand() * 20 + 20).cast("int")) \
        .withColumn("cooling_time_minutes", (rand() * 15 + 15).cast("int")) \
        .withColumn("ambient_humidity", rand() * 30 + 40) \
        .withColumn("freshness_score", 
            (col("ingredient_quality_score") * 0.4 + 
             col("process_adherence_score") * 0.6) / 100) \
        .withColumn("waste_probability", 
            when(col("freshness_score") < 0.85, 0.3)
            .when(col("freshness_score") < 0.90, 0.15)
            .when(col("freshness_score") < 0.95, 0.05)
            .otherwise(0.02))
    
    # Select final columns
    final_features = production_batches.select(
        "feature_id", "product_id", "production_batch_id",
        "production_date", "equipment_id", "baker_id",
        "ingredient_quality_score", "process_adherence_score",
        "proofing_time_minutes", "baking_temperature",
        "baking_time_minutes", "cooling_time_minutes",
        "ambient_humidity", "freshness_score", "waste_probability"
    )
    
    # Write to feature table
    final_features.write.mode("append").saveAsTable("local.gold.fact_product_quality_features")
    
    record_count = final_features.count()
    logger.info(f"Generated {record_count} product quality features")
    
    return record_count


def generate_feature_summary(spark, process_date):
    """Generate summary report of ML features"""
    logger.info("Generating ML feature summary report...")
    
    # Demand forecast summary
    demand_summary = spark.sql(f"""
        SELECT 
            'demand_forecast' as feature_set,
            COUNT(*) as total_features,
            AVG(sales_volume) as avg_sales_volume,
            SUM(CASE WHEN promotion_active THEN 1 ELSE 0 END) as products_on_promotion
        FROM local.gold.fact_demand_forecast_features
        WHERE date = '{process_date}'
    """)
    
    # Equipment maintenance summary
    maintenance_summary = spark.sql(f"""
        SELECT 
            'equipment_maintenance' as feature_set,
            COUNT(*) as total_features,
            AVG(failure_probability) as avg_failure_probability,
            SUM(CASE WHEN failure_probability > 0.7 THEN 1 ELSE 0 END) as high_risk_equipment
        FROM local.gold.fact_equipment_maintenance_features
        WHERE date = '{process_date}'
    """)
    
    # Product quality summary
    quality_summary = spark.sql(f"""
        SELECT 
            'product_quality' as feature_set,
            COUNT(*) as total_features,
            AVG(freshness_score) as avg_freshness_score,
            AVG(waste_probability) as avg_waste_probability
        FROM local.gold.fact_product_quality_features
        WHERE production_date = '{process_date}'
    """)
    
    # Combine summaries
    feature_summary = demand_summary.union(maintenance_summary).union(quality_summary)
    
    logger.info("ML Feature Summary:")
    feature_summary.show()
    
    return feature_summary


def main():
    """Main execution function"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Get process date (default to yesterday)
    process_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    try:
        logger.info(f"Starting ML feature updates for date: {process_date}")
        
        # Update all feature sets
        demand_count = update_demand_forecast_features(spark, process_date)
        maintenance_count = update_equipment_maintenance_features(spark, process_date)
        quality_count = update_product_quality_features(spark, process_date)
        
        # Generate summary report
        feature_summary = generate_feature_summary(spark, process_date)
        
        logger.info(f"ML feature update completed successfully!")
        logger.info(f"Features generated - Demand: {demand_count}, Maintenance: {maintenance_count}, Quality: {quality_count}")
        
    except Exception as e:
        logger.error(f"Error updating ML features: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main() 