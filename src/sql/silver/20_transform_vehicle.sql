-- Silver transformation: vehicle dataset
CREATE OR REPLACE TABLE yinli_catalog.silver.vehicle
AS
WITH staged AS (
  SELECT
    ACCIDENT_NO,
    VEHICLE_ID,
    VEHICLE_YEAR_MANUF,
    VEHICLE_MAKE,
    VEHICLE_BODY_STYLE,
    VEHICLE_TYPE,
    VEHICLE_COLOUR,
    VEHICLE_USE,
    VEHICLE_DAMAGE,
    VEHICLE_STEERING,
    VEHICLE_VIN,
    VEHICLE_MASS,
    VEHICLE_MODEL,
    current_timestamp() AS load_ts,
    ingestion_ts
  FROM yinli_catalog.bronze.vehicle_stream
),
deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY ACCIDENT_NO, VEHICLE_ID ORDER BY load_ts DESC) AS rn
  FROM staged
)
SELECT
  ACCIDENT_NO,
  VEHICLE_ID,
  VEHICLE_YEAR_MANUF,
  VEHICLE_MAKE,
  VEHICLE_BODY_STYLE,
  VEHICLE_TYPE,
  VEHICLE_COLOUR,
  VEHICLE_USE,
  VEHICLE_DAMAGE,
  VEHICLE_STEERING,
  VEHICLE_VIN,
  VEHICLE_MASS,
  VEHICLE_MODEL,
  ingestion_ts,
  load_ts
FROM deduped
WHERE rn = 1;
