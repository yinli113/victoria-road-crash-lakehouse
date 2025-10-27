-- Fact table: vehicle per accident
CREATE OR REPLACE TABLE yinli_catalog.gold.fact_vehicle
AS
SELECT
  v.ACCIDENT_NO,
  v.VEHICLE_ID,
  a.accident_dt,
  a.accident_ts,
  dtime.date_key,
  dcond.condition_key,
  dveh.vehicle_key,
  v.ingestion_ts,
  v.load_ts
FROM yinli_catalog.silver.vehicle v
JOIN yinli_catalog.silver.accident a
  ON v.ACCIDENT_NO = a.ACCIDENT_NO
LEFT JOIN yinli_catalog.gold.dim_time dtime
  ON a.accident_dt = dtime.date_key
LEFT JOIN yinli_catalog.gold.dim_conditions dcond
  ON a.severity = dcond.severity
  AND a.road_geometry = dcond.road_geometry
  AND a.LIGHT_CONDITION = dcond.LIGHT_CONDITION
LEFT JOIN yinli_catalog.gold.dim_vehicle dveh
  ON v.VEHICLE_ID = dveh.vehicle_key;

COMMENT ON TABLE yinli_catalog.gold.fact_vehicle IS 'Vehicle fact table linking to time, conditions, and vehicle dimensions.';
