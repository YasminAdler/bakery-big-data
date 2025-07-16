# Architecture Overview

## System Architecture

```mermaid
graph TB
    subgraph "Data Sources"
        KP[Kafka Producers]
        API[External APIs]
        BATCH[Batch Files]
    end
    
    subgraph "Streaming Layer"
        KAFKA[Apache Kafka]
        KP --> KAFKA
    end
    
    subgraph "Processing Layer"
        SPARK_STREAM[Spark Streaming]
        SPARK_BATCH[Spark Batch]
        KAFKA --> SPARK_STREAM
        BATCH --> SPARK_BATCH
        API --> SPARK_BATCH
    end
    
    subgraph "Storage Layer - MinIO/Iceberg"
        BRONZE[(Bronze Layer)]
        SILVER[(Silver Layer)]
        GOLD[(Gold Layer)]
        
        SPARK_STREAM --> BRONZE
        SPARK_BATCH --> BRONZE
        BRONZE --> SILVER
        SILVER --> GOLD
    end
    
    subgraph "Orchestration"
        AIRFLOW[Apache Airflow]
        AIRFLOW --> SPARK_BATCH
        AIRFLOW --> SPARK_STREAM
    end
    
    subgraph "Analytics & ML"
        BI[BI Tools]
        ML[ML Models]
        GOLD --> BI
        GOLD --> ML
    end
```

## Data Flow Pipeline

```mermaid
graph LR
    subgraph "Real-time Stream"
        SE[Sales Events] --> KT1[Kafka Topic]
        EM[Equipment Metrics] --> KT2[Kafka Topic]
        IU[Inventory Updates] --> KT3[Kafka Topic]
    end
    
    subgraph "Stream Processing"
        KT1 --> SS1[Spark Streaming]
        KT2 --> SS2[Spark Streaming]
        KT3 --> SS3[Spark Streaming]
    end
    
    subgraph "Bronze Layer"
        SS1 --> BSE[Bronze Sales Events]
        SS2 --> BEM[Bronze Equipment Metrics]
        SS3 --> BIU[Bronze Inventory Updates]
    end
    
    subgraph "Silver Layer ETL"
        BSE --> SSE[Silver Sales]
        BEM --> SEM[Silver Equipment]
        BIU --> SIU[Silver Inventory]
    end
    
    subgraph "Gold Layer"
        SSE --> FS[Fact Sales]
        SEM --> FEP[Fact Equipment Performance]
        SIU --> FI[Fact Inventory]
        
        SSE --> DS[Dim Store]
        SSE --> DP[Dim Product]
        SSE --> DC[Dim Customer]
    end
```

## Component Details

### 1. Streaming Infrastructure

```mermaid
graph TB
    subgraph "Kafka Cluster"
        ZK[Zookeeper]
        KB[Kafka Broker]
        ZK --> KB
        
        subgraph "Topics"
            T1[sales-events<br/>3 partitions]
            T2[equipment-metrics<br/>3 partitions]
            T3[inventory-updates<br/>3 partitions]
        end
        
        KB --> T1
        KB --> T2
        KB --> T3
    end
    
    subgraph "Producers"
        P1[Sales Producer]
        P2[Equipment Producer]
        P3[Inventory Producer]
        
        P1 --> T1
        P2 --> T2
        P3 --> T3
    end
```

### 2. Processing Architecture

```mermaid
graph TB
    subgraph "Spark Cluster"
        SM[Spark Master<br/>Port: 7077]
        SW1[Spark Worker 1<br/>2 cores, 2GB]
        SW2[Spark Worker 2<br/>2 cores, 2GB]
        
        SM --> SW1
        SM --> SW2
    end
    
    subgraph "Job Types"
        STR[Streaming Jobs]
        BAT[Batch Jobs]
        
        STR --> SW1
        BAT --> SW2
    end
    
    subgraph "Storage"
        MINIO[MinIO S3<br/>Port: 9000]
        ICE[Iceberg Tables]
        
        SW1 --> MINIO
        SW2 --> MINIO
        MINIO --> ICE
    end
```

### 3. Orchestration Flow

```mermaid
graph TD
    subgraph "Airflow DAGs"
        INIT[bakery_init_infrastructure<br/>One-time setup]
        STREAM[bakery_streaming_manager<br/>Hourly monitoring]
        BATCH[bakery_batch_etl<br/>Daily at 2 AM]
    end
    
    INIT --> |Creates| INFRA[Infrastructure]
    
    STREAM --> |Monitors| SJOBS[Streaming Jobs]
    STREAM --> |Restarts| SJOBS
    
    BATCH --> |Triggers| B2S[Bronze to Silver]
    B2S --> |Then| S2G[Silver to Gold]
    S2G --> |Finally| RPT[Reports]
```

## Data Quality Pipeline

```mermaid
graph LR
    subgraph "Quality Checks"
        RAW[Raw Data] --> QC1{Schema<br/>Validation}
        QC1 -->|Pass| QC2{Business<br/>Rules}
        QC1 -->|Fail| ERR1[Error Log]
        QC2 -->|Pass| QC3{Completeness<br/>Check}
        QC2 -->|Fail| ERR2[Error Log]
        QC3 -->|Pass| CLEAN[Clean Data]
        QC3 -->|Fail| ERR3[Error Log]
    end
    
    CLEAN --> SCORE[Quality Score<br/>0-100]
    SCORE --> META[Metadata]
```

## Late Arrival Handling

```mermaid
sequenceDiagram
    participant P as Producer
    participant K as Kafka
    participant S as Spark
    participant B as Bronze
    participant Si as Silver
    
    P->>K: Event (T-48h)
    Note over K: Event delayed
    P->>K: Event arrives late
    K->>S: Stream processing
    S->>B: Write with watermark
    Note over B: Event time vs Ingestion time
    S->>Si: Reprocess affected window
    Note over Si: Update aggregations
```

## SCD Type 2 Implementation

```mermaid
graph TD
    subgraph "Dimension Update Process"
        NEW[New Record] --> CHK{Changed?}
        CHK -->|No| SKIP[Skip Update]
        CHK -->|Yes| CLOSE[Close Current<br/>end_date = today<br/>is_current = false]
        CLOSE --> INSERT[Insert New<br/>effective_date = today<br/>is_current = true]
    end
    
    subgraph "Historical Tracking"
        REC1[Record v1<br/>2024-01-01 to 2024-03-15<br/>is_current = false]
        REC2[Record v2<br/>2024-03-15 to null<br/>is_current = true]
        REC1 --> REC2
    end
```

## Security Architecture

```mermaid
graph TB
    subgraph "External Access"
        USER[Users]
        API_CLI[API/CLI Access]
    end
    
    subgraph "Authentication Layer"
        AIRFLOW_AUTH[Airflow Auth<br/>Basic Auth]
        MINIO_AUTH[MinIO Auth<br/>Access Keys]
    end
    
    subgraph "Internal Network"
        NET[bakery-network<br/>Docker Network]
        
        subgraph "Services"
            SVC1[Kafka]
            SVC2[Spark]
            SVC3[MinIO]
            SVC4[Airflow]
        end
    end
    
    USER --> AIRFLOW_AUTH
    API_CLI --> MINIO_AUTH
    AIRFLOW_AUTH --> NET
    MINIO_AUTH --> NET
    NET --> SVC1
    NET --> SVC2
    NET --> SVC3
    NET --> SVC4
```

## Scalability Considerations

### Horizontal Scaling

- **Kafka**: Add more brokers and increase partitions
- **Spark**: Add more workers for parallel processing
- **MinIO**: Add more nodes for distributed storage
- **Airflow**: Add more workers for concurrent task execution

### Vertical Scaling

- Increase memory and CPU for Spark executors
- Adjust JVM heap sizes for better performance
- Tune buffer sizes and batch intervals

### Data Partitioning Strategy

- **Bronze**: Partitioned by ingestion date
- **Silver**: Partitioned by event date
- **Gold**: Partitioned by business date
- **Iceberg**: Automatic partition evolution

## Monitoring Points

1. **Kafka Metrics**
   - Consumer lag
   - Message throughput
   - Topic partition distribution

2. **Spark Metrics**
   - Job duration
   - Task failures
   - Memory usage
   - CPU utilization

3. **Data Quality Metrics**
   - Records processed
   - Quality scores
   - Error rates
   - Late arrival statistics

4. **Storage Metrics**
   - MinIO bucket sizes
   - Iceberg table statistics
   - Compaction metrics 