# TL;DR

Define the architecture for a serverless Databricks lakehouse over Unity Catalog with bronze/silver/gold tables and star schema marts for accident analytics.

## 1. Architecture Overview
- Diagram landing (S3) → bronze (external Delta) → silver → gold layers
- Describe compute (serverless SQL Warehouse) and orchestration concepts
- Outline security/governance touchpoints (Unity Catalog, external locations)

## 2. Data Modeling Strategy
- List bronze staging tables (raw accident, vehicle, person if available)
- Define silver normalized tables (column typing, code decoding)
- Specify gold star schema: facts (accident, vehicle, casualty) and dimensions (location, time, conditions, vehicle, outcome)

## 3. Data Flow Details
- Map source fields to target layers (include transformations, lookups)
- Note partitioning/clustering (e.g., by accident date, region)
- Capture incremental load approach (full load vs. appended)

## 4. Infrastructure Components
- Unity Catalog entities: metastore `yinli_metastore`, catalog `yinli_catalog`, schemas (`bronze`, `silver`, `gold`)
- External location config pointing to S3 bucket `yinli-databricks-metastore-root`
- Serverless SQL Warehouse configuration (size, auto-stop policies)

## 5. Access & Security
- Outline privilege model (USAGE, SELECT, MODIFY roles)
- Discuss credential management via Cursor secrets and Databricks personal tokens
- Include auditing/lineage features leveraged

## 6. Future Enhancements
- Placeholder for dbt-based ELT pipeline overlay
- Potential integration with Databricks AI/BI dashboards and ML models
- Scalability considerations for multi-source ingestion
