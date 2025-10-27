-- Silver transformation: standardize accident data
CREATE OR REPLACE TABLE yinli_catalog.silver.accident
AS
WITH staged AS (
  SELECT
    ACCIDENT_NO,
    cast(ACCIDENT_DATE as date) AS accident_dt,
    cast(ACCIDENT_TIME as timestamp) AS accident_ts,
    cast(SEVERITY as int) AS severity,
    cast(SPEED_ZONE as int) AS speed_zone,
    cast(ROAD_GEOMETRY as int) AS road_geometry,
    ROAD_GEOMETRY_DESC,
    cast(DCA_CODE as int) AS dca_code,
    DCA_DESC,
    cast(ACCIDENT_TYPE as int) AS accident_type,
    ACCIDENT_TYPE_DESC,
    NODE_ID,
    DAY_OF_WEEK,
    DAY_WEEK_DESC,
    LIGHT_CONDITION,
    NO_OF_VEHICLES,
    NO_PERSONS,
    NO_PERSONS_KILLED,
    NO_PERSONS_INJ_2,
    NO_PERSONS_INJ_3,
    NO_PERSONS_NOT_INJ,
    POLICE_ATTEND,
    RMA,
    ingestion_ts,
    current_timestamp() AS load_ts
  FROM yinli_catalog.bronze.accident_stream
),
deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY ACCIDENT_NO ORDER BY load_ts DESC) AS rn
  FROM staged
)
SELECT
  ACCIDENT_NO,
  accident_dt,
  accident_ts,
  severity,
  speed_zone,
  road_geometry,
  ROAD_GEOMETRY_DESC,
  dca_code,
  DCA_DESC,
  accident_type,
  ACCIDENT_TYPE_DESC,
  NODE_ID,
  DAY_OF_WEEK,
  DAY_WEEK_DESC,
  LIGHT_CONDITION,
  NO_OF_VEHICLES,
  NO_PERSONS,
  NO_PERSONS_KILLED,
  NO_PERSONS_INJ_2,
  NO_PERSONS_INJ_3,
  NO_PERSONS_NOT_INJ,
  POLICE_ATTEND,
  RMA,
  ingestion_ts,
  load_ts
FROM deduped
WHERE rn = 1;
