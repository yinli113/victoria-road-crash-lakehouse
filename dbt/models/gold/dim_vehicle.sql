{{ config(materialized='table', schema='gold_marts', alias='dim_vehicle') }}

select distinct
    VEHICLE_ID as vehicle_key,
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
from {{ ref('vehicle') }}


