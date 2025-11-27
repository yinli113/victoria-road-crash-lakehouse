{{ config(materialized='table', schema='silver_refined', alias='road_surface_condition') }}

with staged as (
    select
        ACCIDENT_NO,
        cast(`SURFACE_COND` as int) as road_surface_code,
        `SURFACE_COND_DESC` as road_surface_desc,
        cast(`SURFACE_COND_SEQ` as int) as seq,
        ingestion_ts,
        current_timestamp() as load_ts
    from {{ source('bronze', 'road_surface_stream') }}
),
deduped as (
    select *,
           row_number() over (partition by ACCIDENT_NO order by seq asc, load_ts desc) as rn
    from staged
)
select
    ACCIDENT_NO,
    road_surface_code,
    road_surface_desc,
    seq,
    ingestion_ts,
    load_ts
from deduped
where rn = 1


