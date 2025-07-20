erDiagram
    STORE {
        int id
        string location
        int opening_hour
        int closing_hour
    }
    PRODUCT {
        int id
        string name
        float price
        string category
    }
    CUSTOMER {
        string customer_id
    }
    SALES_EVENT {
        string event_id
        datetime event_time
        datetime ingestion_time
        int product_id
        int store_id
        int quantity
        float unit_price
        string customer_id
        string date
        string time_of_day
        string processing_status
    }
    INVENTORY_UPDATE {
        string update_id
        datetime event_time
        datetime ingestion_time
        int product_id
        int store_id
        int beginning_stock
        int restocked_quantity
        int sold_quantity
        int waste_quantity
        string reported_by
        string processing_status
        float late_arrival_hours
    }
    EQUIPMENT {
        int id
        string name
        string type
    }
    EQUIPMENT_METRIC {
        string metric_id
        int equipment_id
        datetime event_time
        datetime ingestion_time
        float power_consumption
        string operational_status
        json raw_payload
        string processing_status
    }

    STORE ||--o{ SALES_EVENT : has
    SALES_EVENT }o--|| PRODUCT : sold
    SALES_EVENT }o--|| CUSTOMER : by
    PRODUCT ||--o{ SALES_EVENT : "sold in"
    CUSTOMER ||--o{ SALES_EVENT : makes
    STORE ||--o{ INVENTORY_UPDATE : has
    PRODUCT ||--o{ INVENTORY_UPDATE : "tracked in"
    EQUIPMENT ||--o{ EQUIPMENT_METRIC : generates
