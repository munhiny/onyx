{{ config(
    materialized='table'
) }}

with source as (
    select *
    from {{ref('silver_egm_data_snap')}}
)

select
    bus_date,
    venue_code,
    egm_description,
    manufacturer,
    fp,
    turnover_sum,
    gmp_sum,
    games_played_sum,
    dbt_valid_from::timestamp as dbt_valid_from,
    coalesce(dbt_valid_to, '9999-12-31 23:59:59 +0000')::timestamp as dbt_valid_to
from source 