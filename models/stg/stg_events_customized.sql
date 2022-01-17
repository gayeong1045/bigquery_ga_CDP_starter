{{ config(
    materialized='incremental',
    partition_by={
      "field": "table_date",
      "data_type": "date"},
    cluster_by= ["table_date","user_pseudo_id","event_name"],
    incremental_strategy = 'insert_overwrite'
)}}

select
  * except(row)
from (
  select
    -- extracts date from source table
    parse_date('%Y%m%d',regexp_extract(_table_suffix,'[0-9]+')) as table_date,
    -- flag to indicate if source table is `events_intraday_`
    case when _table_suffix like '%intraday%' then true else false end as is_intraday,
    *,
    row_number() over (partition by user_pseudo_id, event_name, event_timestamp order by event_timestamp) as row,
    -- 같은시간, 같은 유저, 같은 이벤트가 동시에 생길 수 없다고 가정하고 id 생성
    user_pseudo_id||'_'||event_timestamp||'_'||event_name as event_id
  from
    {{ source ('ga_events', 'events_*')}}
    
  {% if is_incremental() %}
  -- Refresh only recent session data to limit query costs, unless running with --full-refresh
	where regexp_extract(_table_suffix,'[0-9]+') BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('session_lookback_days') }} DAY)) AND
  		FORMAT_DATE("%Y%m%d", CURRENT_DATE())
{% endif %}  

    )
where
  row = 1