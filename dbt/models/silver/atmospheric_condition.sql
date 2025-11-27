{{ config(materialized='table', schema='silver_refined', alias='atmospheric_condition') }}

with staged as (
    select
        ACCIDENT_NO,
        cast(`ATMOSPH_COND` as int) as atmospheric_code,
        `ATMOSPH_COND_DESC` as atmospheric_desc,
        cast(`ATMOSPH_COND_SEQ` as int) as seq,
        ingestion_ts,
        current_timestamp() as load_ts
    from {{ source('bronze', 'atmospheric_stream') }}
),
deduped as (
    select *,
           row_number() over (partition by ACCIDENT_NO order by seq asc, load_ts desc) as rn
    from staged
)
select
    ACCIDENT_NO,
    atmospheric_code,
    atmospheric_desc,
    seq,
    ingestion_ts,
    load_ts
from deduped
where rn = 1


