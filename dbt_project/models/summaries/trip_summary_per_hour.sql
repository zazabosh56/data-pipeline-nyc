{{ config(
    materialized = 'table'
) }}

with enriched as (
    select * from {{ ref('trip_enriched') }}
)

select
    date_trunc('hour', pickup_time) as pickup_hour,
    weather_category,
    count(*) as trip_count,
    avg(trip_duration) as avg_trip_duration,
    avg(tip_percent) as avg_tip_percent
from enriched
group by pickup_hour, weather_category
order by pickup_hour, weather_category
