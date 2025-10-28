# TL;DR

Establish business goals, stakeholders, and success metrics for the Victoria road crash serverless lakehouse. Outline the scope of analytics (trends, risk factors, dashboards) and required data products.

## 1. Objectives & Scope
- Summarize ETL/ELT aims, including ingestion, modeling, and analytics for accident data
- Clarify primary use cases (safety trends, hotspot analysis, policy evaluation)
- Identify key stakeholders and decision-makers

### Business Goals
- Deliver actionable crash insights across accident severity, vehicle involvement, and casualties for VicRoads analytics teams
- Provide policymakers with before/after metrics to evaluate interventions (speed-zone changes, enforcement, infrastructure)
- Enable local councils to explore high-risk locations and conditions to prioritize safety investments
- Supply analysts with AI/BI-ready tables for Genie dashboards and natural-language exploration

### Core Data Products
- Bronze streaming tables: `bronze.accident_stream` (current), `bronze.vehicle_stream`, `bronze.person_stream` (pending data upload)
- Silver canonical tables: `silver.accident`, `silver.vehicle`, `silver.person`
- Gold dimensional model: facts (`fact_accident`, `fact_vehicle`, `fact_person`) and dimensions (`dim_time`, `dim_conditions`, `dim_location`, `dim_vehicle`, `dim_casualty`, `dim_dca`)
- Materialized summaries: `gold.mv_accident_daily`, `gold.mv_severity_distribution`

## 2. Data Sources & Dependencies
- Document raw data location (`s3://yinli-databricks-metastore-root/raw/accident.csv`) and refresh cadence
- Note supporting reference datasets (code tables, weather, geospatial lookups)
- Capture dependencies on Databricks SQL Warehouse, Unity Catalog, and future dbt integration
- Pending uploads: `vehicle.csv`, `person.csv`, code tables for lookup decoding

## 3. Success Criteria
- Define KPIs (e.g., coverage across severity, hotspot accuracy, data freshness SLA)
- List acceptance criteria for bronze/silver/gold data availability (all core tables present, counts validated)
- Include tracing requirements (lineage, auditing)
- Automated tests in `tests/sql` pass after each run

## 4. Constraints & Assumptions
- Serverless SQL Warehouse only (no classic clusters)
- Credentials managed via Cursor environment variables
- Assume dataset remains within ~33 MB per refresh
- Additional raw files will be added under `/raw/vehicle/` and `/raw/person/` when available

## 5. Risks & Mitigations
- Highlight data quality issues (missing timestamps, code interpretation)
- Note operational concerns (credential rotation, cost spikes)
- Provide contingency plans for scaling or dbt adoption
- Risk: missing vehicle/person feeds delays full star schema → mitigation: staged rollout with accident-only analytics

## 6. Open Questions
- Enumerate pending decisions (enrichments, SLA agreements, dashboard tooling)
- Track next steps prior to design/implementation
- Clarify ownership for uploading supplemental datasets and scheduling refreshes
