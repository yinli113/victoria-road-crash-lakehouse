-- Gold dimension: location prototype (initial version without SCD2)
CREATE OR REPLACE TABLE yinli_catalog.gold.dim_location
AS
SELECT DISTINCT
  NODE_ID AS location_key,
  NODE_ID,
  ROAD_GEOMETRY AS road_geometry_code,
  ROAD_GEOMETRY_DESC,
  RMA,
  current_timestamp() AS effective_start_date,
  timestamp('9999-12-31') AS effective_end_date,
  true AS is_current
FROM yinli_catalog.silver.accident;

COMMENT ON TABLE yinli_catalog.gold.dim_location IS 'Location dimension (prototype without SCD2 yet).';
