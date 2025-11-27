{{ config(materialized='table', schema='gold_marts', alias='dim_dca') }}

select distinct
    dca_code,
    DCA_DESC
from {{ ref('accident') }}


