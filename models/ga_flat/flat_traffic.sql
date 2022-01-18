-- incremental, table sufix 등 학습 후 추가 

{{ config(
    materialized='table'
)}}


SELECT
  event_id,
  traffic_source.name as traffic_source_name,
  traffic_source.medium as traffic_source_medium,
  traffic_source.source as traffic_source_site

from {{ref('stg_events_customized')}}
