{{ config(materialized='table', schema='gold_marts', alias='fact_person') }}

select
    p.ACCIDENT_NO,
    p.PERSON_ID,
    a.accident_dt,
    a.accident_ts,
    dtime.date_key,
    dcond.condition_key,
    dcas.casualty_key,
    p.ingestion_ts,
    p.load_ts
from {{ ref('person') }} p
join {{ ref('accident') }} a
    on p.ACCIDENT_NO = a.ACCIDENT_NO
left join {{ ref('dim_time') }} dtime
    on a.accident_dt = dtime.date_key
left join {{ ref('dim_conditions') }} dcond
    on a.severity = dcond.severity
    and a.road_geometry = dcond.road_geometry
    and a.LIGHT_CONDITION = dcond.LIGHT_CONDITION
left join {{ ref('dim_casualty') }} dcas
    on p.PERSON_ID = dcas.casualty_key


