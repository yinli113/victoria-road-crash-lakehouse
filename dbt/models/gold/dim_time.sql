{{ config(materialized='table', schema='gold_marts', alias='dim_time') }}

with time_features as (
    select distinct
        cast(date_format(accident_ts, 'yyyyMMddHH') as bigint) as time_key,
        accident_dt as date_key,
        accident_dt,
        hour(accident_ts) as hour_of_day,
        year(accident_dt) as year,
        month(accident_dt) as month,
        day(accident_dt) as day,
        dayofweek(accident_dt) as day_of_week,
        dayofmonth(accident_dt) as day_of_month,
        dayofyear(accident_dt) as day_of_year,
        weekofyear(accident_dt) as week_of_year,
        date_format(accident_dt, 'EEEE') as weekday_name
    from {{ ref('accident') }}
)
select
    time_key,
    date_key,
    accident_dt,
    hour_of_day,
    year,
    month,
    day,
    day_of_week,
    day_of_month,
    day_of_year,
    week_of_year,
    weekday_name
from time_features


