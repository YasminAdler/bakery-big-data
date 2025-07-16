# Data Model Documentation

## Overview

This document describes the data model for the bakery data pipeline, organized into three layers:
- **Bronze**: Raw data ingestion
- **Silver**: Standardized and enriched data
- **Gold**: Business-ready dimensional model

## Bronze Layer - Raw Data Tables

```mermaid
erDiagram
    bronze_sales_events {
        varchar event_id PK
        timestamp event_time
        timestamp ingestion_time
        int product_id
        int store_id
        int quantity
        decimal unit_price
        varchar customer_id
        date Date
        varchar processing_status
    }
    
    bronze_inventory_updates {
        varchar update_id PK
        timestamp event_time
        timestamp ingestion_time
        int product_id
        int store_id
        int beginning_stock
        int restocked_quantity
        int sold_quantity
        int waste_quantity
        varchar reported_by
        varchar processing_status
    }
    
    bronze_customer_feedback {
        varchar feedback_id PK
        timestamp feedback_time
        timestamp ingestion_time
        varchar customer_id
        int product_id
        int rating
        varchar platform
        varchar review_text
        json raw_payload
        varchar processing_status
    }
    
    bronze_promotions {
        varchar promo_id PK
        int product_id
        varchar promo_type
        date start_date
        date end_date
        decimal discount_percentage
        json raw_payload
        varchar processing_status
    }
    
    bronze_weather_data {
        varchar weather_id PK
        date date
        int store_id
        varchar weather_condition
        json raw_payload
        varchar processing_status
    }
    
    bronze_equipment_metrics {
        varchar metric_id PK
        int equipment_id
        timestamp event_time
        timestamp ingestion_time
        decimal power_consumption
        varchar operational_status
        json raw_payload
        varchar processing_status
    }
```

## Silver Layer - Standardized Data

```mermaid
erDiagram
    silver_sales {
        string sale_id PK
        int product_id
        int store_id
        string customer_id
        date sale_date
        string sale_time
        string time_of_day
        int quantity_sold
        decimal unit_price
        decimal total_revenue
        string promo_id
        string weather_id
        int data_quality_score
        string source_system
        timestamp etl_timestamp
    }
    
    silver_inventory {
        string inventory_id PK
        int product_id
        int store_id
        date inventory_date
        int beginning_stock
        int restocked_quantity
        int sold_quantity
        int waste_quantity
        decimal waste_ratio
        int closing_stock
        decimal days_of_supply
        int data_quality_score
        string source_system
        timestamp etl_timestamp
    }
    
    silver_customer_feedback {
        string feedback_id PK
        string customer_id
        int product_id
        date feedback_date
        string platform
        int rating
        string review_text
        string sentiment_category
        string source_system
        timestamp etl_timestamp
    }
    
    silver_equipment_metrics {
        string metric_id PK
        int equipment_id
        date metric_date
        string metric_time
        decimal power_consumption
        string operational_status
        decimal operational_hours
        boolean maintenance_alert
        string source_system
        timestamp etl_timestamp
    }
```

## Gold Layer - Dimensional Model

### Dimension Tables

```mermaid
erDiagram
    dim_store {
        int store_key PK
        int store_id
        string location
        string type
        string address
        string city
        string region
        string country
        string manager_name
        decimal opening_hour
        decimal closing_hour
        decimal total_area_sqm
        date effective_date
        date end_date
        boolean is_current_flag
        decimal area_sqm
    }
    
    dim_product {
        int product_id PK
        string product_name
        string category
        string subcategory
        string ingredients
        string allergens
        int shelf_life_hours
        boolean is_seasonal
        string nutrition_info
    }
    
    dim_product_pricing {
        int pricing_key PK
        int product_id
        string product_name
        decimal base_price
        string price_category
        string marketing_strategy
        date base_price_creation_date
        date effective_date
        date end_date
        boolean is_current_record
    }
    
    dim_customer {
        int customer_key PK
        string customer_id
        string customer_name
        string email
        string phone
        string address
        string city
        string region
        string country
        date joining_date
        date last_visit_date
        boolean is_current_record
    }
    
    dim_equipment {
        int equipment_id PK
        string equipment_name
        string equipment_type
        string manufacturer
        string model_number
        date purchase_date
        decimal power_consumption_kw
        string maintenance_frequency
        int expected_lifespan_years
        decimal replacement_cost
        boolean is_current
    }
    
    dim_calendar {
        date date PK
        string day_of_week
        int day_of_month
        int month
        string month_name
        int quarter
        int year
        boolean is_weekend
        boolean is_holiday
        string holiday_name
        string season
    }
    
    dim_weather {
        string weather_id PK
        date date
        int store_id
        string weather_condition
        decimal humidity
        decimal wind_speed
    }
    
    dim_marketing_events {
        string event_id PK
        string event_name
        date start_date
        date end_date
        string affected_products
        decimal discount_percentage
        decimal campaign_budget
        string target_audience
        string channel
    }
```

### Fact Tables

```mermaid
erDiagram
    fact_sales {
        string sale_id PK
        int product_id FK
        int store_id FK
        int customer_key FK
        int pricing_key FK
        date date FK
        string weather_id FK
        string marketing_event_id FK
        int quantity_sold
        decimal total_revenue
        string customer_loyalty_tier
        string time_of_day
        boolean promotion_applied
        decimal discount_amount
        decimal profit_margin
        decimal cost_of_goods_sold
        int transaction_sequence
    }
    
    fact_inventory {
        string inventory_id PK
        int product_id FK
        int store_id FK
        date date FK
        int beginning_stock
        int restocked_quantity
        int sold_quantity
        int waste_quantity
        decimal waste_ratio
        decimal days_of_supply
        int stock_out_events
    }
    
    fact_promotions {
        string promo_id PK
        int product_id FK
        string promo_type
        date start_date
        date end_date
        boolean is_active
        decimal discount_percentage
        string target_audience
        decimal sales_lift_percentage
        string description
    }
    
    fact_customer_feedback {
        string feedback_id PK
        int customer_key FK
        int product_id FK
        string platform
        int rating
        string review_text
        date feedback_date
    }
    
    fact_equipment_performance {
        string performance_id PK
        int equipment_id FK
        date date FK
        decimal operational_hours
        decimal avg_temperature
        decimal total_power_consumption
        int maintenance_events
        int downtime_minutes
        decimal efficiency_score
    }
```

### ML Feature Tables

```mermaid
erDiagram
    fact_demand_forecast_features {
        string feature_id PK
        int product_id FK
        int store_id FK
        date date FK
        int sales_volume
        decimal avg_daily_sales
        decimal sales_last_7_days
        decimal sales_last_30_days
        string sales_trend
        decimal seasonality_index
        string day_of_week
        boolean is_holiday
        string holiday_name
        string season
        boolean promotion_active
        string marketing_campaign
        int inventory_level
        decimal waste_ratio
        decimal price
        decimal weather_impact_score
        decimal forecast_accuracy
    }
    
    fact_equipment_maintenance_features {
        string feature_id PK
        int equipment_id FK
        date date FK
        int days_since_last_maintenance
        decimal total_operational_hours
        decimal avg_daily_temperature
        decimal temperature_fluctuation
        decimal power_consumption_trend
        int abnormal_events_count
        int age_in_days
        decimal usage_intensity_score
        decimal maintenance_history_score
        decimal failure_probability
    }
    
    fact_product_quality_features {
        string feature_id PK
        int product_id FK
        string production_batch_id
        date production_date
        int equipment_id FK
        int baker_id
        decimal ingredient_quality_score
        decimal process_adherence_score
        int proofing_time_minutes
        decimal baking_temperature
        int baking_time_minutes
        int cooling_time_minutes
        decimal ambient_humidity
        decimal freshness_score
        decimal waste_probability
    }
```

## Relationships

### Gold Layer Relationships

```mermaid
erDiagram
    fact_sales ||--o{ dim_product : "references"
    fact_sales ||--o{ dim_store : "references"
    fact_sales ||--o{ dim_customer : "references"
    fact_sales ||--o{ dim_product_pricing : "references"
    fact_sales ||--o{ dim_calendar : "references"
    fact_sales ||--o{ dim_weather : "references"
    fact_sales ||--o{ dim_marketing_events : "references"
    
    fact_inventory ||--o{ dim_product : "references"
    fact_inventory ||--o{ dim_store : "references"
    fact_inventory ||--o{ dim_calendar : "references"
    
    fact_promotions ||--o{ dim_product : "references"
    
    fact_customer_feedback ||--o{ dim_customer : "references"
    fact_customer_feedback ||--o{ dim_product : "references"
    
    fact_equipment_performance ||--o{ dim_equipment : "references"
    fact_equipment_performance ||--o{ dim_calendar : "references"
    
    fact_demand_forecast_features ||--o{ dim_product : "references"
    fact_demand_forecast_features ||--o{ dim_store : "references"
    fact_demand_forecast_features ||--o{ dim_calendar : "references"
    
    fact_equipment_maintenance_features ||--o{ dim_equipment : "references"
    fact_equipment_maintenance_features ||--o{ dim_calendar : "references"
    
    fact_product_quality_features ||--o{ dim_product : "references"
    fact_product_quality_features ||--o{ dim_equipment : "references"
```

## Key Design Decisions

### 1. SCD Type 2 Implementation
- **dim_store**: Tracks historical changes to store attributes (manager, hours, area)
- **dim_product_pricing**: Maintains price history with effective dates
- Uses `is_current_flag` and `effective_date`/`end_date` for temporal tracking

### 2. Late Arrival Handling
- Bronze layer includes `event_time` and `ingestion_time` to track delays
- Processing logic can identify records arriving up to 48 hours late
- Silver layer ETL processes update previously processed data

### 3. Data Quality
- Each silver table includes `data_quality_score`
- Validation rules applied during bronze-to-silver transformation
- Failed records stored separately for investigation

### 4. Partitioning Strategy
- Time-based partitioning on date columns for efficient querying
- Fact tables partitioned by date for optimal performance
- Enables efficient data lifecycle management

### 5. ML Feature Engineering
- Pre-computed features for three main use cases:
  - Demand forecasting
  - Equipment maintenance prediction
  - Product quality prediction
- Features updated incrementally as new data arrives 