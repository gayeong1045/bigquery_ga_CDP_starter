-- incremental, table sufix 등 학습 후 추가 

{{ config(
    materialized='table'
)}}

-- vender_id, hostname은 자사 정보만 이용할 경우 삭제 가능

SELECT
  event_id,
  geo.continent,
  geo.country,
  geo.region,
  geo.city,
  geo.sub_continent,
  geo.operating_system,
  geo.metro,

from {{ref('stg_events_customized')}}

