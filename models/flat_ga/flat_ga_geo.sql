-- incremental로 증분 추출/분석 

{{ config(
    materialized='view'
)}}


SELECT
  event_id,
  geo.continent as geo_continent,
  geo.country as geo_country,
  geo.region as geo_region,
  geo.city as geo_city,
  geo.sub_continent as geo_sub_continent,
  geo.metro as geo_metro

from {{ref('stg_ga')}}

