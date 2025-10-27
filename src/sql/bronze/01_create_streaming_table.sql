-- Create or refresh streaming bronze table over raw S3 files
CREATE OR REFRESH STREAMING TABLE yinli_catalog.bronze.accident_stream
SCHEDULE EVERY 1 hour
AS
SELECT
  *,
  current_timestamp() AS ingestion_ts
FROM STREAM read_files(
  's3://yinli-databricks-metastore-root/raw/',
  format => 'csv',
  header => true,
  inferSchema => true
);

-- Snapshot helper table for ad-hoc querying (optional)
CREATE OR REPLACE TABLE yinli_catalog.bronze.accident_snapshot
AS
SELECT * FROM yinli_catalog.bronze.accident_stream;
