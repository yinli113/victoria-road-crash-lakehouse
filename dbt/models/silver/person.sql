{{ config(materialized='table', schema='silver_refined', alias='person') }}

{% set catalog = target.database if target.database is not none else target.catalog %}
{% set person_relation = adapter.get_relation(database=catalog, schema='bronze', identifier='person_stream') %}
{% if not person_relation %}
    {% set person_relation = adapter.get_relation(database=catalog, schema='bronze', identifier='person_snapshot') %}
{% endif %}

{% if person_relation %}
with staged as (
    select
        ACCIDENT_NO,
        PERSON_ID,
        AGE,
        SEX,
        INJURY_SEVERITY,
        SEATING_POSITION,
        ROLE,
        SAFETY_DEVICE,
        BLOOD_ALCOHOL,
        current_timestamp() as load_ts,
        ingestion_ts
    from {{ person_relation }}
),
deduped as (
    select *,
           row_number() over (partition by ACCIDENT_NO, PERSON_ID order by load_ts desc) as rn
    from staged
)
select
    ACCIDENT_NO,
    PERSON_ID,
    AGE,
    SEX,
    INJURY_SEVERITY,
    SEATING_POSITION,
    ROLE,
    SAFETY_DEVICE,
    BLOOD_ALCOHOL,
    ingestion_ts,
    load_ts
from deduped
where rn = 1
{% else %}
select
    cast(null as string) as ACCIDENT_NO,
    cast(null as string) as PERSON_ID,
    cast(null as int) as AGE,
    cast(null as string) as SEX,
    cast(null as string) as INJURY_SEVERITY,
    cast(null as string) as SEATING_POSITION,
    cast(null as string) as ROLE,
    cast(null as string) as SAFETY_DEVICE,
    cast(null as string) as BLOOD_ALCOHOL,
    cast(null as timestamp) as ingestion_ts,
    cast(null as timestamp) as load_ts
where 1 = 0
{% endif %}


