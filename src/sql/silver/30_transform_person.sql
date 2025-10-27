-- Silver transformation: person dataset
CREATE OR REPLACE TABLE yinli_catalog.silver.person
AS
WITH staged AS (
  SELECT
    ACCIDENT_NO,
    PERSON_ID,
    AGE,
    SEX,
    INJURY_SEVERITY,
    SEATING_POSITION,
    ROLE,
    SAFETY_DEVICE,
    BLOOD_ALCOHOL,
    current_timestamp() AS load_ts,
    ingestion_ts
  FROM yinli_catalog.bronze.person_stream
),
deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY ACCIDENT_NO, PERSON_ID ORDER BY load_ts DESC) AS rn
  FROM staged
)
SELECT
  ACCIDENT_NO,
  PERSON_ID,
  AGE,
  SEX,
  INJURY_SEVERITY,
  SEATING_POSITION,
  ROLE,
  SAFETY_DEVICE,
  BLOOD_ALCOHOL,
  ingestion_ts,
  load_ts
FROM deduped
WHERE rn = 1;
