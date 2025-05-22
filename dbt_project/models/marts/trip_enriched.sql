{{ config(
    materialized = 'table'
) }}

with taxi as (
    select
        *
    from {{ ref('source_fact_taxi_trips') }}
),

weather as (
    select
        *,
        date_trunc('hour', timestamp) as weather_hour
    from {{ ref('source_dim_weather') }}
)

select
    taxi.*,
    weather.temperature,
    weather.humidity,
    weather.weather_condition,
    weather.weather_category,
    weather.wind_speed,
    weather.weather_hour as matched_weather_hour
from taxi
left join weather
    on date_trunc('hour', taxi.tpep_pickup_datetime) = weather.weather_hour
    -- Si la colonne pickup s'appelle différemment, remplace par le bon nom !
