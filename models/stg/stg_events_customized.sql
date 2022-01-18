{{ config(
    materialized='table',
    partition_by={
      "field": "event_date",
      "data_type": "date"},
    cluster_by= ["event_date","user_pseudo_id","event_name"],
)}}

select
  *,
  -- 같은시간, 같은 유저, 같은 이벤트가 동시에 생길 수 없다고 가정하고 id 생성
  user_pseudo_id||'_'||event_timestamp||'_'||event_name||row_num as event_id
from (
  select
    *,
    row_number() over (partition by user_pseudo_id, event_name, event_timestamp order by event_timestamp) as row_num
  from
    {{ source ('ga_events', 'events_*')}}

    )