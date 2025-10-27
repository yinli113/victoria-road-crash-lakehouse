-- Dimension: vehicle
CREATE OR REPLACE TABLE yinli_catalog.gold.dim_vehicle
AS
SELECT DISTINCT
  VEHICLE_ID AS vehicle_key,
  VEHICLE_MAKE,
  VEHICLE_MODEL,
  VEHICLE_YEAR_MANUF,
  VEHICLE_BODY_STYLE,
  VEHICLE_TYPE,
  VEHICLE_COLOUR,
  VEHICLE_USE,
  VEHICLE_DAMAGE,
  VEHICLE_STEERING,
  VEHICLE_MASS
FROM yinli_catalog.silver.vehicle;

COMMENT ON TABLE yinli_catalog.gold.dim_vehicle IS 'Vehicle dimension derived from silver vehicle data.';
