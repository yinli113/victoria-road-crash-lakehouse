-- Bronze streaming table for vehicle dataset
CREATE OR REFRESH STREAMING TABLE yinli_catalog.bronze.vehicle_stream
SCHEDULE EVERY 1 hour
AS
SELECT
  *,
  current_timestamp() AS ingestion_ts
FROM STREAM read_files(
  's3://yinli-databricks-metastore-root/raw/vehicle/',
  format => 'csv',
  header => true,
  inferSchema => true
);

CREATE OR REPLACE TABLE yinli_catalog.bronze.vehicle_snapshot AS
SELECT * FROM yinli_catalog.bronze.vehicle_stream;
