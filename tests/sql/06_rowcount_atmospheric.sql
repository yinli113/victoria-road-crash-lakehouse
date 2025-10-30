-- Test: ensure atmospheric_condition silver table is populated
SELECT
  CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS test_result,
  COUNT(*) AS row_count
FROM yinli_catalog.silver.atmospheric_condition;
