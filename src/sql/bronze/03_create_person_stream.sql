-- Bronze streaming table for person dataset
CREATE OR REFRESH STREAMING TABLE yinli_catalog.bronze.person_stream
SCHEDULE EVERY 1 hour
AS
SELECT
  *,
  current_timestamp() AS ingestion_ts
FROM STREAM read_files(
  's3://yinli-databricks-metastore-root/raw/person/',
  format => 'csv',
  header => true,
  inferSchema => true
);

CREATE OR REPLACE TABLE yinli_catalog.bronze.person_snapshot AS
SELECT * FROM yinli_catalog.bronze.person_stream;
