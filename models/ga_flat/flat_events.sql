-- incremental, table sufix 등 학습 후 추가 

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
  params.key,
  params.value.string_value,
  params.value.int_value,
  params.value.float_value,
  params.value.double_value

from {{ref('stg_events_customized')}},
UNNEST(event_params) AS params
