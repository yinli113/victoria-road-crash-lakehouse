{{ config(materialized='table', schema='silver_refined', alias='node') }}

with staged as (
    select
        NODE_ID,
        NODE_TYPE,
        LATITUDE,
        LONGITUDE,
        LGA_NAME,
        DEG_URBAN_NAME,
        POSTCODE_CRASH,
        ingestion_ts,
        current_timestamp() as load_ts
    from {{ source('bronze', 'node_raw') }}
),
deduped as (
    select *,
           row_number() over (partition by NODE_ID order by load_ts desc) as rn
    from staged
)
select
    NODE_ID,
    NODE_TYPE,
    LATITUDE,
    LONGITUDE,
    LGA_NAME,
    DEG_URBAN_NAME,
    POSTCODE_CRASH,
    ingestion_ts,
    load_ts
from deduped
where rn = 1


