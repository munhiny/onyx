{% test is_date_format(model, column_name, date_format='%Y-%m-%d') %}

with validation as (
    select
        {{ column_name }} as date_field
    from {{ model }}
    where {{ column_name }} is not null
        and to_date({{ column_name }}, '{{ date_format }}') is null
)

select *
from validation

{% endtest %} 