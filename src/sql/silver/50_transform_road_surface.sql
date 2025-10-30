-- Silver transformation: road surface condition lookup
CREATE OR REPLACE TABLE yinli_catalog.silver.road_surface_condition
AS
WITH staged AS (
  SELECT
    ACCIDENT_NO,
    CAST(`SURFACE_COND` AS INT) AS road_surface_code,
    `SURFACE_COND_DESC` AS road_surface_desc,
    CAST(`SURFACE_COND_SEQ` AS INT) AS seq,
    ingestion_ts,
    current_timestamp() AS load_ts
  FROM yinli_catalog.bronze.road_surface_stream
),
deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY ACCIDENT_NO ORDER BY seq ASC, load_ts DESC) AS rn
  FROM staged
)
SELECT
  ACCIDENT_NO,
  road_surface_code,
  road_surface_desc,
  seq,
  ingestion_ts,
  load_ts
FROM deduped
WHERE rn = 1;
