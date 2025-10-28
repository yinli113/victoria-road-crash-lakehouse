# AI DevKit Project Rules

## Purpose
Provide project-specific guardrails so Cursor agents follow the Databricks accident lakehouse strategy (serverless SQL Warehouse, bronze/silver/gold on S3).

## Key Principles
- Use serverless Databricks SQL Warehouse for all compute (no classic clusters)
- Store bronze/silver/gold Delta tables in `s3://yinli-databricks-metastore-root/{layer}/`
- Register all tables through Unity Catalog under catalog `yinli_catalog`
- Respect SCD2 requirements for policy-related dimensions
- Align with business goals and core tables listed in `docs/ai/requirements/README.md`
- Consult architecture details in `docs/ai/design/README.md` before major changes

## Data Handling
- Protect credentials; rely on environment variables or secrets
- Avoid committing sensitive tokens
- Validate data quality per docs/ai/testing guidelines before promotion

## Development Workflow
- Update docs under `docs/ai/` as changes occur
- Prefer SQL/Delta transformations checked into repo
- Use Cursor commands for planning, implementation, and review steps
- Log blockers in `docs/ai/issues/README.md` for handoffs

## Future Enhancements
- Leave hooks for dbt integration but keep initial solution Databricks-native
