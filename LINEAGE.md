# dbt Project Lineage

## Data Flow

```
RAW SOURCE DATA
├── globepay_acceptance_transactions
└── globepay_chargeback_transactions
         ↓
    STAGING (views - 1:1 transforms)
    ├── stg_globepay_acceptance_transactions
    └── stg_globepay_chargeback_transactions
         ↓
    INTERMEDIATE (views - business logic)
    ├── int_acceptance_transactions (+ amount_usd calculation)
    └── int_chargeback_transactions
         ↓
    SNAPSHOTS (SCD Type 2 - track changes)
    └── snap_int_chargeback_transactions
         ↓
    MARTS (tables - core analytics)
    └── payments (incremental fact table)
         ↓
    ANALYTICS (tables - business questions)
    ├── part_1_check_acceptance_uniqueness
    ├── part_1_check_chargeback_uniqueness
    ├── part_1_explore_chargeback_distinct_values
    ├── part_2_question_1_acceptance_rate_by_month
    ├── part_2_question_2_countries_high_declined
    └── part_2_question_3_transactions_missing_chargebacks
```

## Schema Organization

| Layer | Schema | Type | Purpose |
|-------|--------|------|---------|
| Raw | SOURCE | Tables | Source data from Globepay |
| Staging | STAGING | Views | 1:1 transforms, data cleaning |
| Intermediate | INTERMEDIATE | Views | Business logic, processor-agnostic design |
| Snapshots | SNAPSHOTS | Tables | SCD Type 2 for mutable chargeback data |
| Marts | MARTS | Tables | Core fact tables (payments) |
| Analytics | ANALYTICS | Tables | Business question models |

## Key Design Decisions

- **Primary Key**: `transaction_ref` (processor-agnostic, supports multi-processor expansion)
- **Materialization**: Views for staging/intermediate (lightweight), tables for marts/analytics (persisted)
- **Incremental Strategy**: MERGE with watermark CTE for efficient updates
- **Currency**: All amounts standardized to USD via `amount_usd` calculation
