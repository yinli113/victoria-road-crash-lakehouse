{{ config(materialized='table', schema='gold_marts', alias='mv_accident_daily') }}

select
    date(accident_ts) as accident_date,
    severity,
    count(*) as accident_count
from {{ ref('fact_accident') }}
group by 1, 2


