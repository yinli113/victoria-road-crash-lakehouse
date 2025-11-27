{{ config(materialized='table', schema='gold_marts', alias='dim_conditions') }}

with joined as (
    select
        s.severity,
        s.speed_zone,
        s.road_geometry,
        s.ROAD_GEOMETRY_DESC,
        s.LIGHT_CONDITION,
        s.DAY_OF_WEEK,
        s.DAY_WEEK_DESC,
        hour(s.accident_ts) as hour_of_day,
        s.RMA,
        atmospheric.atmospheric_code,
        atmospheric.atmospheric_desc,
        road_surface.road_surface_code,
        road_surface.road_surface_desc,
        s.accident_ts
    from {{ ref('accident') }} s
    left join {{ ref('atmospheric_condition') }} atmospheric
        on atmospheric.ACCIDENT_NO = s.ACCIDENT_NO
    left join {{ ref('road_surface_condition') }} road_surface
        on road_surface.ACCIDENT_NO = s.ACCIDENT_NO
)
select distinct
    concat_ws('-',
        coalesce(cast(severity as string), ''),
        coalesce(cast(road_geometry as string), ''),
        coalesce(cast(LIGHT_CONDITION as string), ''),
        coalesce(cast(atmospheric_code as string), ''),
        coalesce(cast(road_surface_code as string), ''),
        coalesce(RMA, ''),
        lpad(cast(hour_of_day as string), 2, '0')
    ) as condition_key,
    severity,
    speed_zone,
    road_geometry,
    ROAD_GEOMETRY_DESC,
    LIGHT_CONDITION,
    DAY_OF_WEEK,
    DAY_WEEK_DESC,
    hour_of_day,
    RMA,
    atmospheric_code,
    atmospheric_desc,
    road_surface_code,
    road_surface_desc
from joined


