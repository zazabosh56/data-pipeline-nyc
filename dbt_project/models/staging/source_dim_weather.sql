{{ config(
    materialized = 'view'
) }}

-- Source : table dim_weather (issue de Spark Streaming)
select
    *
from {{ source('public', 'dim_weather') }}
