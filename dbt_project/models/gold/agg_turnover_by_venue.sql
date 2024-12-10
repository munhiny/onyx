with source as (
    select *
    from {{ ref('silver_egm_data') }}
    where dbt_valid_to = '9999-12-31 23:59:59 +0000'
)

select
sum(turnover_sum) as total_turnover,
venue_code
from source
group by venue_code

