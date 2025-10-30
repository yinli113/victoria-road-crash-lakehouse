-- Dimension: time (date + hour granularity)
CREATE OR REPLACE TABLE yinli_catalog.gold.dim_time
AS
WITH time_features AS (
  SELECT DISTINCT
    CAST(DATE_FORMAT(accident_ts, 'yyyyMMddHH') AS BIGINT) AS time_key,
    accident_dt AS date_key,
    accident_dt,
    HOUR(accident_ts) AS hour_of_day,
    YEAR(accident_dt) AS year,
    MONTH(accident_dt) AS month,
    DAY(accident_dt) AS day,
    DAYOFWEEK(accident_dt) AS day_of_week,
    DAYOFMONTH(accident_dt) AS day_of_month,
    DAYOFYEAR(accident_dt) AS day_of_year,
    WEEKOFYEAR(accident_dt) AS week_of_year,
    DATE_FORMAT(accident_dt, 'EEEE') AS weekday_name
  FROM yinli_catalog.silver.accident
)
SELECT
  time_key,
  date_key,
  accident_dt,
  hour_of_day,
  year,
  month,
  day,
  day_of_week,
  day_of_month,
  day_of_year,
  week_of_year,
  weekday_name
FROM time_features;

COMMENT ON TABLE yinli_catalog.gold.dim_time IS 'Date/hour dimension derived from accident records.';
