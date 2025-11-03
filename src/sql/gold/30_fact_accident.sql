-- Gold fact: accident fact table joining location dimension
CREATE OR REPLACE TABLE yinli_catalog.gold.fact_accident
AS
SELECT
  s.ACCIDENT_NO,
  s.accident_dt,
  s.accident_ts,
  CAST(DATE_FORMAT(s.accident_ts, 'yyyyMMddHH') AS BIGINT) AS time_key,
  s.severity,
  s.speed_zone,
  s.road_geometry,
  s.ROAD_GEOMETRY_DESC,
  s.dca_code,
  s.DCA_DESC,
  s.accident_type,
  s.ACCIDENT_TYPE_DESC,
  s.DAY_OF_WEEK,
  s.DAY_WEEK_DESC,
  s.LIGHT_CONDITION,
  s.NO_OF_VEHICLES,
  s.NO_PERSONS,
  s.NO_PERSONS_KILLED,
  s.NO_PERSONS_INJ_2,
  s.NO_PERSONS_INJ_3,
  s.NO_PERSONS_NOT_INJ,
  s.POLICE_ATTEND,
  s.RMA,
  dim.location_key,
  node.NODE_TYPE,
  node.LATITUDE,
  node.LONGITUDE,
  node.LGA_NAME,
  node.DEG_URBAN_NAME,
  node.POSTCODE_CRASH
FROM yinli_catalog.silver.accident s
LEFT JOIN yinli_catalog.gold.dim_location dim
  ON s.NODE_ID = dim.location_key
  AND dim.is_current
LEFT JOIN yinli_catalog.silver.node node
  ON s.NODE_ID = node.NODE_ID;

COMMENT ON TABLE yinli_catalog.gold.fact_accident IS 'Accident fact table with links to location dimension.';
