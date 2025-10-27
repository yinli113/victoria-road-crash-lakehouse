# TL;DR

Establish business goals, stakeholders, and success metrics for the Victoria road crash serverless lakehouse. Outline the scope of analytics (trends, risk factors, dashboards) and required data products.

## 1. Objectives & Scope
- Summarize ETL/ELT aims, including ingestion, modeling, and analytics for accident data
- Clarify primary use cases (safety trends, hotspot analysis, policy evaluation)
- Identify key stakeholders and decision-makers

## 2. Data Sources & Dependencies
- Document raw data location (`s3://yinli-databricks-metastore-root/raw/accident.csv`) and refresh cadence
- Note supporting reference datasets (code tables, weather, geospatial lookups)
- Capture dependencies on Databricks SQL Warehouse, Unity Catalog, and future dbt integration

## 3. Success Criteria
- Define KPIs (e.g., coverage across severity, hotspot accuracy, data freshness SLA)
- List acceptance criteria for bronze/silver/gold data availability
- Include tracing requirements (lineage, auditing)

## 4. Constraints & Assumptions
- Serverless SQL Warehouse only (no classic clusters)
- Credentials managed via Cursor environment variables
- Assume dataset remains within ~33 MB per refresh

## 5. Risks & Mitigations
- Highlight data quality issues (missing timestamps, code interpretation)
- Note operational concerns (credential rotation, cost spikes)
- Provide contingency plans for scaling or dbt adoption

## 6. Open Questions
- Enumerate pending decisions (enrichments, SLA agreements, dashboard tooling)
- Track next steps prior to design/implementation
