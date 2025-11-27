{{ config(materialized='table', schema='gold_marts', alias='dim_casualty') }}

select distinct
    PERSON_ID as casualty_key,
    AGE,
    SEX,
    INJURY_SEVERITY,
    SEATING_POSITION,
    ROLE,
    SAFETY_DEVICE,
    BLOOD_ALCOHOL
from {{ ref('person') }}


