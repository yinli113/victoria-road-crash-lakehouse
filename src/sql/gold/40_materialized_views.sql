-- Gold materialized views for analytics

CREATE OR REPLACE MATERIALIZED VIEW yinli_catalog.gold.mv_accident_daily
AS
SELECT
  date(accident_ts) AS accident_date,
  severity,
  COUNT(*) AS accident_count
FROM yinli_catalog.gold.fact_accident
GROUP BY 1, 2;

CREATE OR REPLACE MATERIALIZED VIEW yinli_catalog.gold.mv_severity_distribution
AS
SELECT
  severity,
  COUNT(*) AS accident_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM yinli_catalog.gold.fact_accident
GROUP BY severity;

CREATE OR REPLACE MATERIALIZED VIEW yinli_catalog.gold.mv_accident_hourly
AS
SELECT
  DATE(accident_ts) AS accident_date,
  HOUR(accident_ts) AS hour_of_day,
  COUNT(*) AS accident_count
FROM yinli_catalog.gold.fact_accident
GROUP BY DATE(accident_ts), HOUR(accident_ts);
