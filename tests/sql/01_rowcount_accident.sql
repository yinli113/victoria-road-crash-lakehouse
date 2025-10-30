-- Test: ensure silver accident table has one row per ACCIDENT_NO
WITH agg AS (
  SELECT COUNT(*) AS total_rows,
         COUNT(DISTINCT ACCIDENT_NO) AS distinct_accidents
  FROM yinli_catalog.silver.accident
)
SELECT
  CASE WHEN total_rows = distinct_accidents THEN 'PASS' ELSE 'FAIL' END AS test_result,
  total_rows,
  distinct_accidents
FROM agg;
