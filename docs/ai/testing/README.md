# TL;DR

Define testing strategy across data quality, validation, and performance for the lakehouse pipelines.

## 1. Testing Framework
- Enumerate test types (unit SQL checks, data quality assertions, schema validation)
- Identify tools (Databricks SQL tests, notebooks, future dbt tests)
- Track automated SQL tests in `tests/sql/` (`00_run_tests.py`)

## 2. Bronze Layer Tests
- Row count comparisons with raw files
- Schema conformity checks (field presence, data types)
- Landing audit logs

## 3. Silver Layer Tests
- Null/non-null expectations, code vocabulary validation
- Deduplication verification and primary key checks
- Time and location consistency
- Automated checks: `01_rowcount_accident.sql`, `02_schema_accident.sql`

## 4. Gold Layer Tests
- Foreign key integrity between facts/dimensions
- Aggregation sanity (totals vs. source)
- Performance benchmarks for queries
- Automated check: `03_fk_fact_location.sql`

## 5. Automation & Scheduling
- Integrate tests into Databricks Jobs or Workflow tasks
- Alerting/notification pathways on failure
- Schedule `tests/sql/00_run_tests.py` post-load

## 6. Test Documentation & Evidence
- Store test scripts and result logs in repo or Databricks workspace
- Reference acceptance criteria from requirements
