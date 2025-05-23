{{ config(
    materialized = 'table'
) }}

with base as (
    select
        payment_type,
        count(*) as trip_count,
        sum(tip_percent) as total_tip_percent,
        avg(tip_percent) as avg_tip_percent
    from {{ ref('trip_enriched') }}
    where tip_percent is not null
    group by payment_type
)

select
    payment_type as customer_type,
    trip_count,
    total_tip_percent,
    avg_tip_percent
from base
where trip_count > 10
  and avg_tip_percent > 15
