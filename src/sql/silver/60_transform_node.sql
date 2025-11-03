-- Silver transformation: node lookup with coordinates
CREATE OR REPLACE TABLE yinli_catalog.silver.node
AS
WITH staged AS (
  SELECT
    NODE_ID,
    NODE_TYPE,
    LATITUDE,
    LONGITUDE,
    LGA_NAME,
    DEG_URBAN_NAME,
    POSTCODE_CRASH,
    ingestion_ts,
    current_timestamp() AS load_ts
  FROM yinli_catalog.bronze.node_raw
),
deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY NODE_ID ORDER BY load_ts DESC) AS rn
  FROM staged
)
SELECT
  NODE_ID,
  NODE_TYPE,
  LATITUDE,
  LONGITUDE,
  LGA_NAME,
  DEG_URBAN_NAME,
  POSTCODE_CRASH,
  ingestion_ts,
  load_ts
FROM deduped
WHERE rn = 1;
