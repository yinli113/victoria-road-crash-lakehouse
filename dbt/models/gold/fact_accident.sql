{{ config(materialized='table', schema='gold_marts', alias='fact_accident') }}

select
    s.ACCIDENT_NO,
    s.accident_dt,
    s.accident_ts,
    cast(date_format(s.accident_ts, 'yyyyMMddHH') as bigint) as time_key,
    s.severity,
    s.speed_zone,
    s.road_geometry,
    s.ROAD_GEOMETRY_DESC,
    s.dca_code,
    s.DCA_DESC,
    s.accident_type,
    s.ACCIDENT_TYPE_DESC,
    s.DAY_OF_WEEK,
    s.DAY_WEEK_DESC,
    s.LIGHT_CONDITION,
    s.NO_OF_VEHICLES,
    s.NO_PERSONS,
    s.NO_PERSONS_KILLED,
    s.NO_PERSONS_INJ_2,
    s.NO_PERSONS_INJ_3,
    s.NO_PERSONS_NOT_INJ,
    s.POLICE_ATTEND,
    s.RMA,
    dim.location_key,
    node.NODE_TYPE,
    node.LATITUDE,
    node.LONGITUDE,
    node.LGA_NAME,
    node.DEG_URBAN_NAME,
    node.POSTCODE_CRASH
from {{ ref('accident') }} s
left join {{ ref('dim_location') }} dim
    on s.NODE_ID = dim.location_key
    and dim.is_current
left join {{ ref('node') }} node
    on s.NODE_ID = node.NODE_ID


