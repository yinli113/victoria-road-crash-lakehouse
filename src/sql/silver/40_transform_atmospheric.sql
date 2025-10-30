-- Silver transformation: atmospheric condition lookup
CREATE OR REPLACE TABLE yinli_catalog.silver.atmospheric_condition
AS
WITH staged AS (
  SELECT
    ACCIDENT_NO,
    CAST(`ATMOSPH_COND` AS INT) AS atmospheric_code,
    `ATMOSPH_COND_DESC` AS atmospheric_desc,
    CAST(`ATMOSPH_COND_SEQ` AS INT) AS seq,
    ingestion_ts,
    current_timestamp() AS load_ts
  FROM yinli_catalog.bronze.atmospheric_stream
),
deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY ACCIDENT_NO ORDER BY seq ASC, load_ts DESC) AS rn
  FROM staged
)
SELECT
  ACCIDENT_NO,
  atmospheric_code,
  atmospheric_desc,
  seq,
  ingestion_ts,
  load_ts
FROM deduped
WHERE rn = 1;
