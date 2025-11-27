{{ config(materialized='table', schema='gold_marts', alias='mv_severity_distribution') }}

with counts as (
    select
        severity,
        count(*) as accident_count
    from {{ ref('fact_accident') }}
    group by severity
),
total as (
    select sum(accident_count) as total_accidents
    from counts
)
select
    counts.severity,
    counts.accident_count,
    round(counts.accident_count * 100.0 / total.total_accidents, 2) as percentage
from counts
cross join total


