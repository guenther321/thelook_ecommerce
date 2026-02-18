# Project Inventory & Backup

## Complete Project Structure

Generated: 2026-02-18

### Configuration Files
- ✅ dbt_project.yml

### Documentation Files
- ✅ README.md (original challenge description)
- ✅ PART1_DOCUMENTATION.md (Part 1 deliverable)
- ✅ PART2_DOCUMENTATION.md (Part 2 deliverable)
- ✅ LINEAGE.md (data flow & schema organization)
- ✅ ARCHITECTURE.md (5-layer medallion pattern)
- ✅ BEST_PRACTICES.md (macros, validation, documentation)

### dbt Models (11 Total)

**Staging Layer** (2 models)
- models/staging/stg_globepay_acceptance_transactions.sql
- models/staging/stg_globepay_acceptance_transactions.yml
- models/staging/stg_globepay_chargeback_transactions.sql
- models/staging/stg_globepay_chargeback_transactions.yml
- models/staging/_sources.yml

**Intermediate Layer** (2 models)
- models/intermediate/int_acceptance_transactions.sql
- models/intermediate/int_acceptance_transactions.yml
- models/intermediate/int_chargeback_transactions.sql
- models/intermediate/int_chargeback_transactions.yml

**Marts Layer** (1 model)
- models/marts/payments.sql
- models/marts/payments.yml

**Analytics Layer** (6 models)
- models/analytics/part_1_check_acceptance_uniqueness.sql
- models/analytics/part_1_check_chargeback_uniqueness.sql
- models/analytics/part_1_explore_chargeback_distinct_values.sql
- models/analytics/part_2_question_1_acceptance_rate_by_month.sql
- models/analytics/part_2_question_2_countries_high_declined.sql
- models/analytics/part_2_question_3_transactions_missing_chargebacks.sql

### Snapshots (1 model)
- snapshots/snap_int_chargeback_transactions.sql
- snapshots/snap_int_chargeback_transactions.yml

### Macros
- macros/generate_schema_name.sql (schema naming override)

### Tests
- tests/test_amount_usd_not_null.sql
- Total: 22/22 tests passing ✅

### Data Summary
- Source Data: Globepay payment processing (Q1-Q2 2019)
- Total Transactions: 5,430
- Chargebacks: 223 (4.1%)
- No Chargebacks: 5,207 (95.9%)
- Geographic Coverage: Multiple countries (US, CA, AE, UK, FR, MX, etc.)

### Branch Information
- Repository: dbt-starter-project
- Owner: guenther321
- Current Branch: feature/ggaiser-deel-analytics-challenge
- Commit Message: "Complete Deel Analytics Challenge - Part 1 & Part 2 with full documentation and 22/22 tests passing"

### Snowflake Integration
- Account: vrmfrpd-yf69384
- Database: DEEL_ANALYTICS
- Schemas: STAGING, INTERMEDIATE, MARTS, ANALYTICS, SNAPSHOTS
- Warehouse: COMPUTE_WH

### Key Design Decisions
1. Primary Key: transaction_ref (processor-agnostic, multi-processor ready)
2. Currency Standardization: All amounts in USD (amount_usd calculated)
3. Incremental Strategy: MERGE with watermark CTE
4. SCD Type 2: Snapshot for chargeback history tracking
5. Materialization: Views (staging/intermediate), Tables (marts/analytics)

---
**Everything confirmed intact and ready for submission.**
