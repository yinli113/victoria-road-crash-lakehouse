-- Bronze streaming table for atmospheric condition lookup
CREATE OR REFRESH STREAMING TABLE yinli_catalog.bronze.atmospheric_stream
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
  fileNamePattern => 'Atmospheric Condition.csv',
  schema => '_id STRING, ACCIDENT_NO STRING, ATMOSPH_COND INT, ATMOSPH_COND_SEQ INT, ATMOSPH_COND_DESC STRING'
);

CREATE OR REPLACE TABLE yinli_catalog.bronze.atmospheric_snapshot AS
SELECT * FROM yinli_catalog.bronze.atmospheric_stream;
