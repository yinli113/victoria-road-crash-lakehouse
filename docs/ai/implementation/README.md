# TL;DR

Detail implementation steps for building the bronze/silver/gold pipelines, including SQL scripts, Delta table creation, and automation patterns.

## 1. Environment Preparation
- Configure Cursor secrets and Databricks CLI/SQL connections
- Validate access to S3 external location and Unity Catalog
- Set up notebooks or SQL files for transformations

## 2. Ingestion (Bronze)
- Steps to create external Delta tables over raw CSV
- Metadata capture (schema, load timestamp, file tracking)
- Handling of schema drift or late-arriving data

## 3. Transformation (Silver)
- SQL scripts to standardize data types, decode codes, enrich location data
- Surrogate key generation and normalization patterns
- Incremental load logic (MERGE, deduplication)

## 4. Aggregation (Gold)
- Build fact/dimension tables and empower star schema
- Materialized views vs. Delta tables for analytics
- Indexing/optimization (ZORDER, partitioning)

## 5. SQL Patterns & Examples
- **Streaming bronze ingestion** kept declarative with Lakeflow streaming tables to auto-refresh from S3 (executed; ~188k rows)

```sql
CREATE OR REFRESH STREAMING TABLE yinli_catalog.bronze.accident_stream
  LOCATION 's3://yinli-databricks-metastore-root/bronze/accident_stream'
  SCHEDULE EVERY 1 hour
  AS
SELECT *
FROM STREAM read_files(
  's3://yinli-databricks-metastore-root/raw/accident.csv',
  format => 'csv',
  header => true
);
```

- **Silver normalization** follows a CTE-first pattern and writes to a Delta table in the silver path

```sql
CREATE OR REPLACE TABLE yinli_catalog.silver.accident
  LOCATION 's3://yinli-databricks-metastore-root/silver/accident'
AS
WITH staged AS (
  SELECT
    accident_no,
    to_timestamp(accident_date, 'yyyyMMdd') AS accident_dt,
    severity,
    speed_zone,
    road_geometry,
    load_ts
  FROM yinli_catalog.bronze.accident_stream
),
deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY accident_no ORDER BY load_ts DESC) AS rn
  FROM staged
)
SELECT accident_no, accident_dt, severity, speed_zone, road_geometry
FROM deduped
WHERE rn = 1;
```

- **SCD2 merge** handles policy changes (for example, speed-zone updates)

```sql
MERGE INTO yinli_catalog.silver.dim_location tgt
USING yinli_catalog.silver.dim_location_staging src
ON tgt.location_id = src.location_id AND tgt.is_current
WHEN MATCHED AND (tgt.speed_zone <> src.speed_zone) THEN
  UPDATE SET
    tgt.effective_end_date = src.effective_start_date - INTERVAL 1 DAY,
    tgt.is_current = false
WHEN NOT MATCHED THEN
  INSERT (location_id, speed_zone, effective_start_date, effective_end_date, is_current)
  VALUES (src.location_id, src.speed_zone, src.effective_start_date, '9999-12-31', true);
```

- **Gold summaries** can leverage materialized views for high-traffic aggregates (created; row counts verified via tests)

```sql
CREATE OR REPLACE MATERIALIZED VIEW yinli_catalog.gold.mv_accident_daily
  LOCATION 's3://yinli-databricks-metastore-root/gold/mv_accident_daily'
AS
SELECT
  date(accident_dt) AS accident_date,
  severity,
  COUNT(*) AS accident_count
FROM yinli_catalog.silver.accident
GROUP BY 1, 2;
```

## 6. Orchestration & Automation
- Outline manual vs. workflow-triggered runs (Databricks Jobs, SQL tasks)
- Integration hooks for future dbt models
- Run `tests/sql/00_run_tests.py` after pipelines to ensure row counts and FK checks

## 7. Documentation & Version Control
- Maintain SQL scripts/notebooks in repo, with clear naming
- Reference docs in design/planning sections for traceability
