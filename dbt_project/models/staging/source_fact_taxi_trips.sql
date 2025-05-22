{{ config(
    materialized = 'view'
) }}

-- Source : table fact_taxi_trips (issue de Spark)
select
    *
from {{ source('public', 'fact_taxi_trips') }}
