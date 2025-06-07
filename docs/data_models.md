Bronze Layer Tables
mermaiderDiagram
    bronze_sales_events {
        string event_id PK
        timestamp event_time
        timestamp ingestion_time
        int product_id
        int store_id
        int quantity
        decimal unit_price
        string customer_id
        string time_of_day
        json raw_payload
        string processing_status
        timestamp processing_timestamp
        string data_source
    }
    
    bronze_inventory_updates {
        string update_id PK
        timestamp event_time
        timestamp ingestion_time
        int product_id
        int store_id
        int beginning_stock
        int restocked_quantity
        int sold_quantity
        int waste_quantity
        string reported_by
        json raw_payload
        string processing_status
        timestamp processing_timestamp
        boolean is_late_arrival
    }
    
    bronze_equipment_metrics {
        string metric_id PK
        int equipment_id
        timestamp event_time
        timestamp ingestion_time
        decimal temperature
        decimal power_consumption
        string operational_status
        json sensor_readings
        json raw_payload
        string processing_status
    }
Silver Layer Tables
mermaiderDiagram
    silver_sales {
        string sale_id PK
        int product_id FK
        int store_id FK
        string customer_id FK
        date sale_date
        time sale_time
        string time_of_day
        int quantity_sold
        decimal unit_price
        decimal total_revenue
        string promo_id FK
        string weather_id FK
        int data_quality_score
        string source_system
        timestamp etl_timestamp
        decimal cost_of_goods_sold
        decimal profit_margin
    }
    
    silver_inventory {
        string inventory_id PK
        int product_id FK
        int store_id FK
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
        decimal inventory_value
        string stock_status
    }
Gold Layer Dimension Tables
mermaiderDiagram
    dim_product_pricing {
        int pricing_key PK
        int product_id FK
        string product_name
        decimal base_price
        string price_category
        string marketing_strategy
        date valid_from
        date valid_to
        boolean is_current_record
        decimal margin_percentage
        string pricing_tier
    }
    
    dim_customer {
        int customer_key PK
        string customer_id
        string customer_name
        string email
        string phone
        string loyalty_tier
        decimal lifetime_value
        date first_purchase_date
        date valid_from
        date valid_to
        boolean is_current_record
    }
Gold Layer Fact Tables
mermaiderDiagram
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
    
    fact_demand_forecast_features {
        string feature_id PK
        int product_id FK
        int store_id FK
        date date FK
        int sales_volume
        decimal avg_daily_sales
        decimal sales_last_7_days
        decimal sales_last_30_days
        decimal sales_trend
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