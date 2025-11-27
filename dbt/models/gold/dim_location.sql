{{ config(materialized='table', schema='gold_marts', alias='dim_location') }}

select distinct
    NODE_ID as location_key,
    NODE_ID,
    road_geometry as road_geometry_code,
    ROAD_GEOMETRY_DESC,
    RMA,
    current_timestamp() as effective_start_date,
    timestamp('9999-12-31') as effective_end_date,
    true as is_current
from {{ ref('accident') }}


