# TL;DR

End-to-end steps to connect Cursor (CLI) to Databricks using a serverless SQL Warehouse and store configuration for the accident lakehouse project.

## 1. Prerequisites
- Databricks workspace with Unity Catalog enabled (`yinli_metastore`, `yinli_catalog`)
- Serverless SQL Warehouse (small size, auto-stop) on the **Current** channel
- Personal Access Token (PAT) or OAuth client credentials
- Python 3.13+ (macOS) and `databricks-cli` installed

## 2. Install Databricks CLI
- Ensure `python3` and `pip` are present: `python3 -m pip --version`
- Install CLI: `python3 -m pip install --upgrade databricks-cli`

## 3. Configure CLI Profile
- Run `databricks configure --profile accident`
  - Paste host (base URL only): `https://dbc-becaefdf-cf1c.cloud.databricks.com`
  - Paste PAT when prompted
- Verify profile in `~/.databrickscfg` under `[accident]`

## 4. Persist Workspace Metadata
- Store workspace URL in repo: `config/databricks_workspace_url.txt`
- Capture SQL Warehouse ID (from console) for later CLI use

## 5. Test Connectivity
- List warehouses: `databricks sql warehouses list --profile accident`
- Execute sanity query: `databricks sql query execute --profile accident --warehouse-id <ID> --sql "SELECT 1"`

## 6. Secure Practices
- Keep PATs out of version control
- Rotate tokens periodically
- Use environment variables in Cursor sessions for warehouse ID, catalog, schema

## 7. Next Steps
- Register bronze streaming table pointing to `s3://yinli-databricks-metastore-root/raw/`
- Implement silver/gold transformations using SQL scripts in repo
- Prepare for dbt integration if desired
