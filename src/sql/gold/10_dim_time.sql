-- Dimension: time
CREATE OR REPLACE TABLE yinli_catalog.gold.dim_time
AS
SELECT DISTINCT
  accident_dt AS date_key,
  accident_dt,
  YEAR(accident_dt) AS year,
  MONTH(accident_dt) AS month,
  DAY(accident_dt) AS day,
  DAYOFWEEK(accident_dt) AS day_of_week,
  DAYOFMONTH(accident_dt) AS day_of_month,
  DAYOFYEAR(accident_dt) AS day_of_year,
  WEEKOFYEAR(accident_dt) AS week_of_year,
  DATE_FORMAT(accident_dt, 'EEEE') AS weekday_name
FROM yinli_catalog.silver.accident;

COMMENT ON TABLE yinli_catalog.gold.dim_time IS 'Date dimension derived from accident records.';
