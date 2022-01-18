-- incremental로 증분 추출/분석 

{{ config(
    materialized='incremental',
    partition_by={
      "field": "table_date",
      "data_type": "date"},
    cluster_by= ["table_date","user_pseudo_id","event_name"],
    incremental_strategy = 'insert_overwrite'
)}}

select
  *,
  --join key 생성
  user_pseudo_id||'_'||event_timestamp||'_'||event_name||'_'||row_num as event_id
from (
  select
    *,
    -- 이벤트가 실행된 시간이 아닌 ga가 데이터를 불러온 각 시간을 table_date로 지정
    parse_date('%Y%m%d',regexp_extract(_table_suffix,'[0-9]+')) as table_date,
    row_number() over (partition by user_pseudo_id, event_name, event_timestamp order by event_timestamp) as row_num
  from
    {{ source ('ga_events', 'events_*')}}
  -- config가 incremental이면 3일 전부터 오늘까지의 데이터 불러오기
  -- 최근 데이터만 불러들이는 효과 - 쿼리 비용 줄이기 위함
  {% if is_incremental() %}
    where regexp_extract(_table_suffix,'[0-9]+') BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('session_lookback_days') }} DAY)) AND
      FORMAT_DATE("%Y%m%d", CURRENT_DATE())
  {% endif %} 

    )