flowchart TD
    A[Raw Data Ingestion] --> B[Bronze Table]
    B --> C[Run Quality Checks e.g. nulls, schema]
    C --> D{Valid?}
    D -- Yes --> E[Promote to Silver]
    D -- No --> F[Log Error / Notify / Quarantine]