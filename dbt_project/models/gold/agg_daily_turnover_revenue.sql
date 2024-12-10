with source as (
    select *
    from {{ ref('silver_egm_data') }}
    where dbt_valid_to = '9999-12-31 23:59:59 +0000'
)

select
bus_date,
sum(turnover_sum) as total_turnover,
sum(gmp_sum) as total_revenue
from source
group by bus_date
order by bus_date
