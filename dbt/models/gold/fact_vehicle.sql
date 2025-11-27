{{ config(materialized='table', schema='gold_marts', alias='fact_vehicle') }}

select
    v.ACCIDENT_NO,
    v.VEHICLE_ID,
    a.accident_dt,
    a.accident_ts,
    dtime.date_key,
    dcond.condition_key,
    dveh.vehicle_key,
    v.ingestion_ts,
    v.load_ts
from {{ ref('vehicle') }} v
join {{ ref('accident') }} a
    on v.ACCIDENT_NO = a.ACCIDENT_NO
left join {{ ref('dim_time') }} dtime
    on a.accident_dt = dtime.date_key
left join {{ ref('dim_conditions') }} dcond
    on a.severity = dcond.severity
    and a.road_geometry = dcond.road_geometry
    and a.LIGHT_CONDITION = dcond.LIGHT_CONDITION
left join {{ ref('dim_vehicle') }} dveh
    on v.VEHICLE_ID = dveh.vehicle_key


