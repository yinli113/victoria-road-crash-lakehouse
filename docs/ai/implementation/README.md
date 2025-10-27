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

## 5. Orchestration & Automation
- Outline manual vs. workflow-triggered runs (Databricks Jobs, SQL tasks)
- Integration hooks for future dbt models

## 6. Documentation & Version Control
- Maintain SQL scripts/notebooks in repo, with clear naming
- Reference docs in design/planning sections for traceability
