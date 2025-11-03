-- Bronze table for node lookup (coordinates)
CREATE OR REPLACE TABLE yinli_catalog.bronze.node_raw
AS
SELECT
  *,
  current_timestamp() AS ingestion_ts
FROM read_files(
  's3://yinli-databricks-metastore-root/raw/',
  format => 'csv',
  header => true,
  schema => 'ACCIDENT_NO STRING, NODE_ID INT, NODE_TYPE STRING, AMG_X DOUBLE, AMG_Y DOUBLE, LGA_NAME STRING, LGA_NAME_ALL STRING, DEG_URBAN_NAME STRING, LATITUDE DOUBLE, LONGITUDE DOUBLE, POSTCODE_CRASH INT',
  fileNamePattern => 'node.csv'
);
