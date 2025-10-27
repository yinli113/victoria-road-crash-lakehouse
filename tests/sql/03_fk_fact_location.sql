-- Test: ensure fact_accident joins a location dimension row
SELECT
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS test_result,
  COUNT(*) AS unmatched_count
FROM yinli_catalog.gold.fact_accident f
LEFT JOIN yinli_catalog.gold.dim_location d
  ON f.location_key = d.location_key
WHERE d.location_key IS NULL;
