---
config:
 layout: elk
 theme: neutral
---
flowchart TD
subgraph Orchestration["Orchestration"]
       O1["Airflow Scheduler & DAGs"]
 end
subgraph Streaming["Streaming"]
       S1["Kafka Producers"]
       S2["Kafka Topics"]
       S3["Kafka Broker"]
 end
subgraph Processing["Processing"]
       P1["Spark Streaming Jobs"]
       P2["Spark Batch ETL Jobs"]
       P3["Bronze Layer Raw Data"]
       P4["Silver Layer Cleaned Data"]
       P5["Gold Layer Business Data"]
       P6["MinIO & Iceberg Tables"]
       P7["ML Features"]
       P8["Business Reports / Dashboard"]
 end
   O1 --> P2 & P1 & P7 & P8
   S1 --> S2
   S2 --> S3
   S3 --> P1
   P1 --> P3
   P2 --> P3
   P3 --> P4 & P6
   P4 --> P5 & P6
   P5 --> P7 & P8 & P6



