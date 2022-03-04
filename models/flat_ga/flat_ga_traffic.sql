
{{ config(
    materialized='view'
)}}


with traffic as (
  SELECT
    event_id,
    event_date,
    timestamp_micros(event_timestamp) as event_time,
    event_name,
    user_pseudo_id,
    user_id,
    traffic_source.name as traffic_source_name,
    traffic_source.medium as traffic_source_medium,
    traffic_source.source as traffic_source_site

  from {{ref('stg_ga')}}
)

select distinct traffic_source_medium from traffic
where traffic_source_site = "SA"

