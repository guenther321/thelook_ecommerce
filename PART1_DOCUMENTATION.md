# Part 1: Data Exploration & Architecture

## Preliminary Data Exploration

**Uniqueness Validation** (via `part_1_check_acceptance_uniqueness.sql`)
- ✓ Acceptance transactions: 5,430 total, 5,430 unique (100% - no duplicates)
- ✓ Primary key (`transaction_ref`) is valid and unique

**Chargeback Uniqueness** (via `part_1_check_chargeback_uniqueness.sql`)
- ✓ Chargeback records: 5,430 total, 5,430 unique (100% - no duplicates)
- ✓ Chargeback primary key design validated

**Chargeback Characteristics** (via `part_1_explore_chargeback_distinct_values.sql`)
- **Status**: All chargebacks have status = TRUE (confirmed chargebacks only)
- **Source**: All chargebacks from GLOBALPAY (single processor)
- **Finding**: 100% data consistency in chargeback metadata

---

## Model Architecture

### Medallion Pattern (5 Layers)

**Layer 1: Staging** (STAGING schema)
- `stg_globepay_acceptance_transactions`: 1:1 clean from raw
- `stg_globepay_chargeback_transactions`: 1:1 clean from raw
- Materialization: Views (lightweight)

**Layer 2: Intermediate** (INTERMEDIATE schema)
- `int_acceptance_transactions`: Processor-agnostic layer + USD conversion
- `int_chargeback_transactions`: Filters confirmed chargebacks
- Materialization: Views
- **Key Design**: `transaction_ref` as universal primary key

**Layer 3: Snapshots** (SNAPSHOTS schema)
- `snap_int_chargeback_transactions`: SCD Type 2 for chargeback history
- Tracks: `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`
- Materialization: Tables

**Layer 4: Marts** (MARTS schema)
- `payments`: Core fact table (incremental MERGE strategy)
- Joins acceptance + chargeback data
- Materialization: Tables
- **Primary Key**: `transaction_ref` (processor-agnostic)

**Layer 5: Analytics** (ANALYTICS schema)
- Part 1 Exploration (3 models)
- Part 2 Business Questions (3 models)
- Materialization: Tables

---

## Key Design Patterns

### 1. Multi-Processor Ready Architecture
- **Challenge**: Data comes from Globepay now, but future expansion to Stripe/PayPal needed
- **Solution**: `transaction_ref` as universal primary key (not processor-specific `external_ref`)
- **Benefit**: New processor data can join on same primary key without refactoring

### 2. Currency Standardization
- **Challenge**: Transactions in multiple currencies, need USD for analysis
- **Solution**: `amount_usd` calculated from: `amount_local / exchange_rate_from_json`
- **Benefit**: Consistent reporting across geographies

### 3. Incremental Merge Strategy
- **Challenge**: Chargebacks can appear after transaction (mutable data)
- **Solution**: Incremental MERGE with watermark CTE
- **Logic**: Only process new transactions + existing rows with chargeback changes
- **Benefit**: Efficient updates, historical accuracy

### 4. SCD Type 2 Snapshots
- **Challenge**: Need to track when chargebacks occurred
- **Solution**: Snapshot table with `dbt_valid_from`/`dbt_valid_to`
- **Benefit**: Full audit trail of chargeback status changes

---

## Data Quality & Testing

**Test Coverage: 22/22 passing** ✓

- Unique primary keys (`transaction_ref`)
- Not null on critical columns
- Column type validation
- Relationship integrity

---

## Files Overview

| File | Purpose |
|------|---------|
| LINEAGE.md | Data flow diagram and schema organization |
| ARCHITECTURE.md | 5-layer medallion pattern explanation |
| BEST_PRACTICES.md | Macros, validation, documentation patterns |
| models/staging/ | Raw data cleaning (2 models) |
| models/intermediate/ | Business logic (2 models) |
| models/marts/ | Core fact tables (1 model) |
| snapshots/ | Historical tracking (1 model) |
| models/analytics/ | Business questions (6 models) |
