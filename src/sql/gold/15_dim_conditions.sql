-- Dimension: conditions (lighting, road, severity, etc.)
CREATE OR REPLACE TABLE yinli_catalog.gold.dim_conditions
AS
SELECT DISTINCT
  CONCAT_WS('-',
    COALESCE(CAST(severity AS STRING), ''),
    COALESCE(CAST(road_geometry AS STRING), ''),
    COALESCE(CAST(LIGHT_CONDITION AS STRING), ''),
    COALESCE(CAST(atmospheric.atmospheric_code AS STRING), ''),
    COALESCE(CAST(road_surface.road_surface_code AS STRING), ''),
    COALESCE(RMA, ''),
    LPAD(CAST(HOUR(accident_ts) AS STRING), 2, '0')
  ) AS condition_key,
  severity,
  speed_zone,
  road_geometry,
  ROAD_GEOMETRY_DESC,
  LIGHT_CONDITION,
  DAY_OF_WEEK,
  DAY_WEEK_DESC,
  HOUR(accident_ts) AS hour_of_day,
  RMA,
  atmospheric.atmospheric_code,
  atmospheric.atmospheric_desc,
  road_surface.road_surface_code,
  road_surface.road_surface_desc
FROM yinli_catalog.silver.accident s
LEFT JOIN yinli_catalog.silver.atmospheric_condition atmospheric
  ON atmospheric.ACCIDENT_NO = s.ACCIDENT_NO
LEFT JOIN yinli_catalog.silver.road_surface_condition road_surface
  ON road_surface.ACCIDENT_NO = s.ACCIDENT_NO;

COMMENT ON TABLE yinli_catalog.gold.dim_conditions IS 'Environmental and road conditions dimension including hour-of-day, atmospheric, and road surface context.';
