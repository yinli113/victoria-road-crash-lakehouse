{{ config(materialized='table', schema='silver_refined', alias='accident') }}

with staged as (
    select
        ACCIDENT_NO,
        cast(ACCIDENT_DATE as date) as accident_dt,
        case
            when ACCIDENT_DATE is not null then try_to_timestamp(
                concat(
                    ACCIDENT_DATE,
                    lpad(
                        case
                            when ACCIDENT_TIME is null then '0000'
                            else regexp_replace(ACCIDENT_TIME, '\\D', '')
                        end,
                        4,
                        '0'
                    )
                ),
                'yyyyMMddHHmm'
            )
            else null
        end as accident_ts,
        cast(SEVERITY as int) as severity,
        cast(SPEED_ZONE as int) as speed_zone,
        cast(ROAD_GEOMETRY as int) as road_geometry,
        ROAD_GEOMETRY_DESC,
        cast(DCA_CODE as int) as dca_code,
        DCA_DESC,
        cast(ACCIDENT_TYPE as int) as accident_type,
        ACCIDENT_TYPE_DESC,
        NODE_ID,
        DAY_OF_WEEK,
        DAY_WEEK_DESC,
        LIGHT_CONDITION,
        NO_OF_VEHICLES,
        NO_PERSONS,
        NO_PERSONS_KILLED,
        NO_PERSONS_INJ_2,
        NO_PERSONS_INJ_3,
        NO_PERSONS_NOT_INJ,
        POLICE_ATTEND,
        RMA,
        ingestion_ts,
        current_timestamp() as load_ts
    from {{ source('bronze', 'accident_stream') }}
),
deduped as (
    select *,
           row_number() over (partition by ACCIDENT_NO order by load_ts desc) as rn
    from staged
)
select
    ACCIDENT_NO,
    accident_dt,
    accident_ts,
    severity,
    speed_zone,
    road_geometry,
    ROAD_GEOMETRY_DESC,
    dca_code,
    DCA_DESC,
    accident_type,
    ACCIDENT_TYPE_DESC,
    NODE_ID,
    DAY_OF_WEEK,
    DAY_WEEK_DESC,
    LIGHT_CONDITION,
    NO_OF_VEHICLES,
    NO_PERSONS,
    NO_PERSONS_KILLED,
    NO_PERSONS_INJ_2,
    NO_PERSONS_INJ_3,
    NO_PERSONS_NOT_INJ,
    POLICE_ATTEND,
    RMA,
    ingestion_ts,
    load_ts
from deduped
where rn = 1


