-- Bootstrap Unity Catalog catalog and schemas

-- Create catalog (idempotent)
CREATE CATALOG IF NOT EXISTS yinli_catalog;

-- Create bronze/silver/gold schemas (idempotent)
CREATE SCHEMA IF NOT EXISTS yinli_catalog.bronze;
CREATE SCHEMA IF NOT EXISTS yinli_catalog.silver;
CREATE SCHEMA IF NOT EXISTS yinli_catalog.gold;

-- Optional: comment metadata
COMMENT ON CATALOG yinli_catalog IS 'Accident analytics lakehouse';
COMMENT ON SCHEMA yinli_catalog.bronze IS 'Raw landing streaming tables';
COMMENT ON SCHEMA yinli_catalog.silver IS 'Cleansed conforming tables';
COMMENT ON SCHEMA yinli_catalog.gold IS 'Analytics marts & materialized views';
