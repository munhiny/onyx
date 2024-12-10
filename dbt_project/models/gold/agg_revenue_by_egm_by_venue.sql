with source as (
    select *
    from {{ ref('silver_egm_data') }}
    where dbt_valid_to = '9999-12-31 23:59:59 +0000'
)

select
egm_description,
venue_code,
sum(gmp_sum) as total_revenue
from source
group by egm_description, venue_code 
order by egm_description, venue_code