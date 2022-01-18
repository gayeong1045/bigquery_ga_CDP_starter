-- incremental로 증분 추출/분석 

{{ config(
    materialized='table'
)}}


SELECT
  event_id,
  traffic_source.name as traffic_source_name,
  traffic_source.medium as traffic_source_medium,
  traffic_source.source as traffic_source_site

from {{ref('stg_events_customized')}}
