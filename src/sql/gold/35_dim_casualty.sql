-- Dimension: casualty/person info
CREATE OR REPLACE TABLE yinli_catalog.gold.dim_casualty
AS
SELECT DISTINCT
  PERSON_ID AS casualty_key,
  AGE,
  SEX,
  INJURY_SEVERITY,
  SEATING_POSITION,
  ROLE,
  SAFETY_DEVICE,
  BLOOD_ALCOHOL
FROM yinli_catalog.silver.person;

COMMENT ON TABLE yinli_catalog.gold.dim_casualty IS 'Casualty/person dimension.';
