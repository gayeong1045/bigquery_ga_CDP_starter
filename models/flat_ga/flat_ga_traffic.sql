-- incremental로 증분 추출/분석 

{{ config(
    materialized='view'
)}}


SELECT
  event_id,
  event_date,
  event_name,
  user_pseudo_id,
  user_id,
  traffic_source.name as traffic_source_name,
  traffic_source.medium as traffic_source_medium,
  traffic_source.source as traffic_source_site

from {{ref('stg_ga')}}
