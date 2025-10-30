-- Bronze streaming table for road surface condition lookup
CREATE OR REFRESH STREAMING TABLE yinli_catalog.bronze.road_surface_stream
SCHEDULE EVERY 1 hour
AS
SELECT
  *,
  current_timestamp() AS ingestion_ts
FROM STREAM read_files(
  's3://yinli-databricks-metastore-root/raw/',
  format => 'csv',
  header => true,
  inferSchema => true,
  fileNamePattern => 'Road Surface Condition.csv',
  schema => '_id STRING, ACCIDENT_NO STRING, SURFACE_COND INT, SURFACE_COND_DESC STRING, SURFACE_COND_SEQ INT'
);

CREATE OR REPLACE TABLE yinli_catalog.bronze.road_surface_snapshot AS
SELECT * FROM yinli_catalog.bronze.road_surface_stream;
