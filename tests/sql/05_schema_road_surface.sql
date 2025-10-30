-- Test: verify columns in silver road_surface_condition table
WITH expected AS (
  SELECT explode(array(
    'ACCIDENT_NO','ROAD_SURFACE_CODE','ROAD_SURFACE_DESC','SEQ','INGESTION_TS','LOAD_TS'
  )) AS column_name
),
actual AS (
  SELECT upper(column_name) AS column_name
  FROM system.information_schema.columns
  WHERE lower(table_catalog) = 'yinli_catalog'
    AND lower(table_schema) = 'silver'
    AND lower(table_name) = 'road_surface_condition'
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS test_result,
  COLLECT_LIST(column_name) AS missing_columns
FROM expected
LEFT JOIN actual USING (column_name)
WHERE actual.column_name IS NULL;
