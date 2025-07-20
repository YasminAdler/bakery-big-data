sequenceDiagram
    participant POS as POS System
    participant IoT as IoT Sensors
    participant Inventory as Inventory System
    participant Kafka as Kafka
    participant Spark as Spark Job
    participant Iceberg as Iceberg Tables
    participant Airflow as Airflow
    participant Output as ML/Dashboard


    POS->>Kafka: Send sales events
    IoT->>Kafka: Send equipment metrics
    Inventory->>Kafka: Send inventory updates


    Kafka->>Spark: Ingest data (stream + batch)
    Spark->>Iceberg: Write to Bronze
    Airflow->>Spark: Trigger transformation job
    Spark->>Iceberg: Bronze → Silver
    Spark->>Iceberg: Silver → Gold
    Iceberg->>Output: Serve to dashboard or ML
