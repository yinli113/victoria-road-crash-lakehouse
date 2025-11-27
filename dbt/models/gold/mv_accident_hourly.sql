{{ config(materialized='table', schema='gold_marts', alias='mv_accident_hourly') }}

select
    date(accident_ts) as accident_date,
    hour(accident_ts) as hour_of_day,
    count(*) as accident_count
from {{ ref('fact_accident') }}
group by date(accident_ts), hour(accident_ts)


