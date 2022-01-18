-- incremental, table sufix 등 학습 후 추가 

{{ config(
    materialized='table'
)}}

select
  *,
  --join key 생성
  user_pseudo_id||'_'||event_timestamp||'_'||event_name||row_num as event_id
from (
  select
    *,
    row_number() over (partition by user_pseudo_id, event_name, event_timestamp order by event_timestamp) as row_num
  from
    {{ source ('ga_events', 'events_*')}}

    )