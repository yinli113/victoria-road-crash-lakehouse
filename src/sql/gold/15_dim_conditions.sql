-- Dimension: conditions (lighting, road, severity, etc.)
CREATE OR REPLACE TABLE yinli_catalog.gold.dim_conditions
AS
SELECT DISTINCT
  CONCAT_WS('-', COALESCE(CAST(severity AS STRING), ''), COALESCE(CAST(road_geometry AS STRING), ''), COALESCE(CAST(LIGHT_CONDITION AS STRING), '')) AS condition_key,
  severity,
  speed_zone,
  road_geometry,
  ROAD_GEOMETRY_DESC,
  LIGHT_CONDITION,
  DAY_OF_WEEK,
  DAY_WEEK_DESC
FROM yinli_catalog.silver.accident;

COMMENT ON TABLE yinli_catalog.gold.dim_conditions IS 'Environmental and road conditions dimension.';
