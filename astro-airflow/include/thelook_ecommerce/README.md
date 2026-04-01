# TheLook eCommerce — dbt Project

## Purpose

This repo is a **sandbox** for practising and applying various analytics engineering skills in preparation for an interview as **Lead Analytics Engineer** (contractor) for a 6–12 month project between **Monzo** and **Mastercard**.

## Job Description

### Data Warehouse Re-Architecture Project

Monzo is looking for a Lead Analytics Engineer (contractor) to assist in a data warehouse migration of data assets and pipelines to a new architecture.

The work will involve embedding in an existing data team within a given business domain (e.g. Payments, Borrowing, Finance, etc.) and defining and building key data assets for that area. This will require strong data modelling skills and ability to quickly absorb and elicit business context. The tooling involved will be a combination of open source, cloud and in-house data engineering tools with a focus on SQL data modelling (namely dbt SQL, orchestrated in Airflow and executed in BigQuery).

The re-architecture is not intended to be like-for-like and so will therefore involve understanding the business requirements in order to identify model-generation patterns, safely backfill historical data, and enable zero-downtime cutovers for downstream users. The objective is to unblock dependent analytics teams quickly while reducing long-term cost, duplication, and architectural complexity.

This project involves close collaboration with other data and business practitioners, leveraging existing ways of working, development and testing frameworks, and tooling to deliver with high consistency and quality.

### Key Technical Skills

- Strong track record of data modelling, ETL and data pipelines and scaling data warehousing infrastructure
- Expert level skills in SQL, data modelling, data warehousing concepts and working with large petabyte scale datasets
- Deep familiarity with common data engineering tools, particularly dbt, Airflow and cloud data warehouses such as BigQuery / Snowflake
- Experience working with data streams from various services, such as financial, transactional, and operational backend systems
- Experience building robust and reliable data sets that require a high level of control and correctness
- Ability to think strategically about business context and need — experience working closely with product or business squads to build out data architecture for insights or reporting purposes

### Behaviours and Soft Skills

- Able to work with cross-functional, fast-moving teams and balancing priorities and technical trade-offs
- Enabling other Data Scientists and Analytics Engineers to excel by sharing expertise on data architecture, as well as influencing backend engineering decisions
- Work closely with backend engineering and data platform team to define the data architecture across systems in a unified manner
- Collaboratively set standards across data at Monzo, fostering knowledge sharing and continuously improving data practices
- Contribute to prioritising data governance issues, ensuring a comprehensive approach to data integrity and compliance

## Practice Roadmap

### P0 — Must know (core of the role)
- [ ] Advanced incremental strategies — `merge`, `insert_overwrite`, microbatch, late-arriving data
- [ ] Star schema / dimensional modelling — fact and dimension tables at scale
- [ ] Modelling financial/transactional data — handling payment streams, statuses, and event-driven data
- [ ] Safe historical backfill strategies — incremental models with full refresh, partition-based backfill
- [ ] Data reconciliation — comparing old vs new pipeline outputs to ensure correctness
- [ ] Partitioning and clustering — optimal strategies for petabyte-scale tables in BigQuery
- [ ] Custom macros — reusable SQL generators, dynamic SQL, Jinja patterns
- [ ] dbt packages — dbt_utils, dbt_expectations, dbt_elementary

### P1 — Very important (will likely come up in interview)
- [ ] Model-generation patterns — reusable macros that generate models (e.g. codegen for staging)
- [ ] Slowly Changing Dimensions (SCD Type 2) — dbt snapshots for tracking historical changes
- [ ] Zero-downtime cutovers — running old and new pipelines in parallel, validation before switching
- [ ] Schema change handling — `on_schema_change` strategies, contract enforcement in dbt
- [ ] dbt contracts and model governance — access controls, model versions, groups
- [ ] Unit testing — validating transformation logic in isolation (dbt unit tests)
- [ ] Data contracts — enforcing schemas between producers and consumers
- [ ] Source freshness checks — `dbt source freshness`
- [ ] Airflow basics — DAGs, operators, scheduling, dependencies
- [ ] dbt + Airflow integration — running dbt from Airflow (Cosmos, BashOperator)

### P2 — Good to know (differentiators)
- [ ] Reducing duplication and complexity — refactoring redundant models, DRY principles
- [ ] BigQuery cost optimization — slot reservations, query optimization, materialization choices
- [ ] Nested/repeated fields (STRUCT/ARRAY) — common in BigQuery, useful for denormalization
- [ ] Advanced testing — dbt_expectations for distribution checks, volume anomalies, row-level validation
- [ ] Data observability — Elementary dashboards, lineage tracking
- [ ] Tags, selectors, and CI strategies — efficient `dbt build` in large projects
- [ ] Documentation standards — comprehensive YML docs, column-level descriptions
- [ ] Naming conventions and style guide — contributing guidelines for a team setting

### P3 — Nice to have
- [ ] Exposures — documenting downstream dependencies (dashboards, ML models)
- [ ] Seeds — managing reference/lookup data
- [ ] Monitoring and alerting — using Elementary for pipeline observability
