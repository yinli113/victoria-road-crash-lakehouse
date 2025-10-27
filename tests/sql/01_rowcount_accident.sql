-- Test: ensure silver accident count matches bronze stream count
WITH bronze_cnt AS (
  SELECT COUNT(*) AS cnt FROM yinli_catalog.bronze.accident_stream
),
silver_cnt AS (
  SELECT COUNT(*) AS cnt FROM yinli_catalog.silver.accident
)
SELECT CASE WHEN bronze_cnt.cnt = silver_cnt.cnt THEN 'PASS' ELSE 'FAIL' END AS test_result,
       bronze_cnt.cnt AS bronze_count,
       silver_cnt.cnt AS silver_count
FROM bronze_cnt CROSS JOIN silver_cnt;
