-- incremental로 증분 추출/분석 

{{ config(
    materialized='table'
)}}

-- unique한 event_params_value 22개의 값을 int로 가지는 6개 변수
{% set int_events = ['engaged_session_event' ,'ga_session_id' ,'ga_session_number' ,'engagement_time_msec' ,'entrances' ,'percent_scrolled'] %}
-- unique한 event_params_value 22개의 값을 str로 가지는 15개 변수
{% set str_events = ['page_location' ,'page_referrer' ,'page_title' ,'campaign' ,'content' ,'ignore_referrer' ,'medium' ,'source' ,'term' ,'search' ,'gclid' ,'link_url' ,'link_classes' ,'link_domain' ,'outbound'] %}
-- session_engaged는 int, str 중복

SELECT
  event_id,
  event_name,
  params.key as event_params_key,
  params.value.string_value as event_params_value_string,
  params.value.int_value as event_params_value_int,
  params.value.float_value as event_params_value_float,
  params.value.double_value as event_params_value_double

from {{ref('stg_events_customized')}},
UNNEST(event_params) AS params
