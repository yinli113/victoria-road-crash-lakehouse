-- Gold fact: accident fact table joining location dimension
CREATE OR REPLACE TABLE yinli_catalog.gold.fact_accident
AS
SELECT
  s.ACCIDENT_NO,
  s.accident_dt,
  s.accident_ts,
  s.severity,
  s.speed_zone,
  s.road_geometry,
  s.dca_code,
  s.accident_type,
  dim.location_key,
  dim.RMA
FROM yinli_catalog.silver.accident s
LEFT JOIN yinli_catalog.gold.dim_location dim
  ON s.NODE_ID = dim.location_key
  AND dim.is_current;

COMMENT ON TABLE yinli_catalog.gold.fact_accident IS 'Accident fact table with links to location dimension.';
