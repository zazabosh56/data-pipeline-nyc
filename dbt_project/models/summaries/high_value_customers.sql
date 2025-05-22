{{ config(
    materialized = 'table'
) }}

with base as (
    select
        passenger_count,
        sum(total_amount) as total_spent,
        count(*) as trip_count,
        avg(tip_percent) as avg_tip_percent
    from {{ ref('trip_enriched') }}
    where passenger_count is not null
    group by passenger_count
)

select
    passenger_count as customer_id,
    trip_count,
    total_spent,
    avg_tip_percent
from base
where trip_count > 10
  and total_spent > 300
  and avg_tip_percent > 15
