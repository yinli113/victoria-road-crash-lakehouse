{{ config(materialized='table', schema='silver_refined', alias='vehicle') }}

{% set catalog = target.database if target.database is not none else target.catalog %}
{% set vehicle_relation = adapter.get_relation(database=catalog, schema='bronze', identifier='vehicle_stream') %}
{% if not vehicle_relation %}
    {% set vehicle_relation = adapter.get_relation(database=catalog, schema='bronze', identifier='vehicle_snapshot') %}
{% endif %}

{% if vehicle_relation %}
with staged as (
    select
        ACCIDENT_NO,
        VEHICLE_ID,
        VEHICLE_YEAR_MANUF,
        VEHICLE_MAKE,
        VEHICLE_BODY_STYLE,
        VEHICLE_TYPE,
        VEHICLE_COLOUR,
        VEHICLE_USE,
        VEHICLE_DAMAGE,
        VEHICLE_STEERING,
        VEHICLE_VIN,
        VEHICLE_MASS,
        VEHICLE_MODEL,
        current_timestamp() as load_ts,
        ingestion_ts
    from {{ vehicle_relation }}
),
deduped as (
    select *,
           row_number() over (partition by ACCIDENT_NO, VEHICLE_ID order by load_ts desc) as rn
    from staged
)
select
    ACCIDENT_NO,
    VEHICLE_ID,
    VEHICLE_YEAR_MANUF,
    VEHICLE_MAKE,
    VEHICLE_BODY_STYLE,
    VEHICLE_TYPE,
    VEHICLE_COLOUR,
    VEHICLE_USE,
    VEHICLE_DAMAGE,
    VEHICLE_STEERING,
    VEHICLE_VIN,
    VEHICLE_MASS,
    VEHICLE_MODEL,
    ingestion_ts,
    load_ts
from deduped
where rn = 1
{% else %}
select
    cast(null as string) as ACCIDENT_NO,
    cast(null as string) as VEHICLE_ID,
    cast(null as int) as VEHICLE_YEAR_MANUF,
    cast(null as string) as VEHICLE_MAKE,
    cast(null as string) as VEHICLE_BODY_STYLE,
    cast(null as string) as VEHICLE_TYPE,
    cast(null as string) as VEHICLE_COLOUR,
    cast(null as string) as VEHICLE_USE,
    cast(null as string) as VEHICLE_DAMAGE,
    cast(null as string) as VEHICLE_STEERING,
    cast(null as string) as VEHICLE_VIN,
    cast(null as double) as VEHICLE_MASS,
    cast(null as string) as VEHICLE_MODEL,
    cast(null as timestamp) as ingestion_ts,
    cast(null as timestamp) as load_ts
where 1 = 0
{% endif %}


