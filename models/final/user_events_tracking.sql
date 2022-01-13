{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date"},
    cluster_by= ["event_date", "user_pseudo_id", "event_name"],
    incremental_strategy = 'insert_overwrite'
)}}


SELECT
    parse_date("%Y%m%d", event_date) as event_date,
    TIMESTAMP_MICROS(event_timestamp) as event_time,
    user_id,
    user_pseudo_id,
    if(params.key = 'ga_session_id', params.value.int_value, null) ga_session_id,
    event_name,
    params.key AS event_parameter_key

from {{ref('stg_events_customized')}},
UNNEST(event_params) AS params
