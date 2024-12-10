{% snapshot silver_egm_data_snap %}

{{ config(
    target_schema='silver',
    strategy='check',
    unique_key='unique_key',
    check_cols=['turnover_sum','gmp_sum','games_played_sum']
) }}

with source as (
    select * from {{source('bronze', 'landing_egm_data')}}
    {% if is_incremental() %}
        where updated_at > (select max(updated_at) from {{this}})
    {% endif %}
),

raw_egm_data as (
    select
        concat(bus_date, venue_code, egm_description, manufacturer, fp) as unique_key,  
        bus_date,
        venue_code,
        egm_description,
        manufacturer,
        fp,
        turnover_sum,
        gmp_sum,
        games_played_sum,
        updated_at
    from source
)

select * from raw_egm_data

{% endsnapshot %}