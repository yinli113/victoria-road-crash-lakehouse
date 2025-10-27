-- Test: verify expected columns exist in silver accident table
WITH expected AS (
  SELECT explode(array(
    'ACCIDENT_NO','ACCIDENT_DT','ACCIDENT_TS','SEVERITY','SPEED_ZONE','ROAD_GEOMETRY',
    'ROAD_GEOMETRY_DESC','DCA_CODE','DCA_DESC','ACCIDENT_TYPE','ACCIDENT_TYPE_DESC',
    'NODE_ID','DAY_OF_WEEK','DAY_WEEK_DESC','LIGHT_CONDITION','NO_OF_VEHICLES',
    'NO_PERSONS','NO_PERSONS_KILLED','NO_PERSONS_INJ_2','NO_PERSONS_INJ_3',
    'NO_PERSONS_NOT_INJ','POLICE_ATTEND','RMA','INGESTION_TS','LOAD_TS'
  )) AS column_name
),
actual AS (
  SELECT upper(column_name) AS column_name
  FROM system.information_schema.columns
  WHERE lower(table_catalog) = 'yinli_catalog'
    AND lower(table_schema) = 'silver'
    AND lower(table_name) = 'accident'
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS test_result,
  COLLECT_LIST(column_name) AS missing_columns
FROM expected
LEFT JOIN actual USING (column_name)
WHERE actual.column_name IS NULL;
