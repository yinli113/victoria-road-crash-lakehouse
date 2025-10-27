-- Fact table: person/casualty per accident
CREATE OR REPLACE TABLE yinli_catalog.gold.fact_person
AS
SELECT
  p.ACCIDENT_NO,
  p.PERSON_ID,
  a.accident_dt,
  a.accident_ts,
  dtime.date_key,
  dcond.condition_key,
  dcas.casualty_key,
  p.ingestion_ts,
  p.load_ts
FROM yinli_catalog.silver.person p
JOIN yinli_catalog.silver.accident a
  ON p.ACCIDENT_NO = a.ACCIDENT_NO
LEFT JOIN yinli_catalog.gold.dim_time dtime
  ON a.accident_dt = dtime.date_key
LEFT JOIN yinli_catalog.gold.dim_conditions dcond
  ON a.severity = dcond.severity
  AND a.road_geometry = dcond.road_geometry
  AND a.LIGHT_CONDITION = dcond.LIGHT_CONDITION
LEFT JOIN yinli_catalog.gold.dim_casualty dcas
  ON p.PERSON_ID = dcas.casualty_key;

COMMENT ON TABLE yinli_catalog.gold.fact_person IS 'Person fact table linking casualties to time and conditions dimensions.';
