-- Dimension: DCA (crash classification)
CREATE OR REPLACE TABLE yinli_catalog.gold.dim_dca
AS
SELECT DISTINCT
  dca_code,
  DCA_DESC
FROM yinli_catalog.silver.accident;

COMMENT ON TABLE yinli_catalog.gold.dim_dca IS 'DCA crash classification dimension.';
