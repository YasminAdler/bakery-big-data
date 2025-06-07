from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)

class DataQualityChecker:
    """Comprehensive data quality checker for Spark DataFrames"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def check_completeness(self, df: DataFrame, required_columns: List[str]) -> Dict:
        """Check for null values in required columns"""
        results = {}
        
        for col_name in required_columns:
            null_count = df.filter(col(col_name).isNull()).count()
            total_count = df.count()
            completeness_ratio = (total_count - null_count) / total_count if total_count > 0 else 0
            
            results[col_name] = {
                "null_count": null_count,
                "completeness_ratio": completeness_ratio,
                "passed": completeness_ratio >= 0.95
            }
        
        return results
    
    def check_uniqueness(self, df: DataFrame, unique_columns: List[str]) -> Dict:
        """Check for duplicate values in columns that should be unique"""
        results = {}
        
        for col_name in unique_columns:
            total_count = df.count()
            distinct_count = df.select(col_name).distinct().count()
            uniqueness_ratio = distinct_count / total_count if total_count > 0 else 0
            
            # Find duplicates
            duplicates = df.groupBy(col_name) \
                .count() \
                .filter(col("count") > 1) \
                .count()
            
            results[col_name] = {
                "distinct_count": distinct_count,
                "duplicate_count": duplicates,
                "uniqueness_ratio": uniqueness_ratio,
                "passed": duplicates == 0
            }
        
        return results
    
    def check_validity(self, df: DataFrame, validity_rules: Dict) -> Dict:
        """Check if values meet validity criteria"""
        results = {}
        
        for col_name, rules in validity_rules.items():
            invalid_count = 0
            
            if "min_value" in rules:
                invalid_count += df.filter(col(col_name) < rules["min_value"]).count()
            
            if "max_value" in rules:
                invalid_count += df.filter(col(col_name) > rules["max_value"]).count()
            
            if "allowed_values" in rules:
                invalid_count += df.filter(~col(col_name).isin(rules["allowed_values"])).count()
            
            if "pattern" in rules:
                invalid_count += df.filter(~col(col_name).rlike(rules["pattern"])).count()
            
            total_count = df.count()
            validity_ratio = (total_count - invalid_count) / total_count if total_count > 0 else 0
            
            results[col_name] = {
                "invalid_count": invalid_count,
                "validity_ratio": validity_ratio,
                "passed": validity_ratio >= 0.99
            }
        
        return results
    
    def check_consistency(self, df: DataFrame, consistency_rules: List[Tuple]) -> Dict:
        """Check cross-column consistency"""
        results = {}
        
        for rule_name, condition in consistency_rules:
            violations = df.filter(~condition).count()
            total_count = df.count()
            consistency_ratio = (total_count - violations) / total_count if total_count > 0 else 0
            
            results[rule_name] = {
                "violations": violations,
                "consistency_ratio": consistency_ratio,
                "passed": consistency_ratio >= 0.99
            }
        
        return results
    
    def check_timeliness(self, df: DataFrame, date_column: str, max_delay_hours: int) -> Dict:
        """Check if data is timely"""
        current_time = current_timestamp()
        
        late_records = df.filter(
            (datediff(current_time, col(date_column)) * 24) > max_delay_hours
        ).count()
        
        total_count = df.count()
        timeliness_ratio = (total_count - late_records) / total_count if total_count > 0 else 0
        
        return {
            "late_records": late_records,
            "timeliness_ratio": timeliness_ratio,
            "passed": timeliness_ratio >= 0.95
        }
    
    def generate_quality_report(self, df: DataFrame, table_name: str, checks: Dict) -> DataFrame:
        """Generate comprehensive quality report"""
        report_data = []
        
        # Run all checks
        if "required_columns" in checks:
            completeness = self.check_completeness(df, checks["required_columns"])
            for col, result in completeness.items():
                report_data.append({
                    "table_name": table_name,
                    "check_type": "completeness",
                    "column_name": col,
                    "metric_value": result["completeness_ratio"],
                    "passed": result["passed"],
                    "details": f"Null count: {result['null_count']}"
                })
        
        if "unique_columns" in checks:
            uniqueness = self.check_uniqueness(df, checks["unique_columns"])
            for col, result in uniqueness.items():
                report_data.append({
                    "table_name": table_name,
                    "check_type": "uniqueness",
                    "column_name": col,
                    "metric_value": result["uniqueness_ratio"],
                    "passed": result["passed"],
                    "details": f"Duplicates: {result['duplicate_count']}"
                })
        
        # Create report DataFrame
        report_df = self.spark.createDataFrame(report_data)
        
        # Add metadata
        report_df = report_df.withColumn("check_timestamp", current_timestamp()) \
                           .withColumn("record_count", lit(df.count()))
        
        return report_df

# Example usage for different layers
def get_bronze_quality_checks():
    """Define quality checks for bronze layer"""
    return {
        "sales_events": {
            "required_columns": ["event_id", "product_id", "store_id", "quantity", "unit_price"],
            "unique_columns": ["event_id"],
            "validity_rules": {
                "quantity": {"min_value": 1, "max_value": 100},
                "unit_price": {"min_value": 0.01, "max_value": 1000}
            }
        },
        "inventory_updates": {
            "required_columns": ["update_id", "product_id", "store_id", "event_time"],
            "unique_columns": ["update_id"],
            "validity_rules": {
                "beginning_stock": {"min_value": 0},
                "waste_quantity": {"min_value": 0}
            }
        }
    }

def get_silver_quality_checks():
    """Define quality checks for silver layer"""
    return {
        "sales": {
            "required_columns": ["sale_id", "product_id", "store_id", "total_revenue"],
            "consistency_rules": [
                ("revenue_calculation", col("total_revenue") == col("quantity_sold") * col("unit_price")),
                ("profit_margin_range", (col("profit_margin") >= 0) & (col("profit_margin") <= 100))
            ]
        },
        "inventory": {
            "consistency_rules": [
                ("stock_balance", 
                 col("closing_stock") == col("beginning_stock") + col("restocked_quantity") 
                 - col("sold_quantity") - col("waste_quantity")),
                ("waste_ratio_range", (col("waste_ratio") >= 0) & (col("waste_ratio") <= 1))
            ]
        }
    }

def get_gold_quality_checks():
    """Define quality checks for gold layer"""
    return {
        "fact_sales": {
            "referential_integrity": [
                ("product_id", "dim_product"),
                ("store_id", "dim_store"),
                ("customer_key", "dim_customer")
            ],
            "aggregation_accuracy": [
                ("daily_revenue", "SUM(total_revenue) GROUP BY date, store_id")
            ]
        },
        "dim_product_pricing": {
            "scd_validity": [
                ("no_overlaps", "valid_from < valid_to"),
                ("single_current", "COUNT(is_current_record=true) = 1 per product_id")
            ]
        }
    }