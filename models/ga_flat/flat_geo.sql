-- incremental, table sufix 등 학습 후 추가 

{{ config(
    materialized='table'
)}}


SELECT
  event_id,
  geo.continent as geo_continent,
  geo.country as geo_country,
  geo.region as geo_region,
  geo.city as geo_city,
  geo.sub_continent as geo_sub_continent,
  geo.metro as geo_metro

from {{ref('stg_events_customized')}}

