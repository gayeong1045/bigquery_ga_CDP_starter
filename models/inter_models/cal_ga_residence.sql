with event_info as (
    select 
        event_time,
        event_name,
        user_pseudo_id,
        user_id,
        max(if(event_params_key = 'ga_session_id', event_params_value_int, null)) as ga_session_id,  
        max(if(event_params_key = 'page_location', event_params_value_string, null)) as page_location,
    from {{ ref('flat_ga_events') }}
    where event_name = 'page_view'
    group by event_time, event_name, user_pseudo_id, user_id
    
), 

session_sort as (
    select distinct
        cast(event_time as datetime) as event_time,
        page_location,
        user_pseudo_id,
        user_id,
        ga_session_id
    from event_info
),

session_lead as (
    select 
        *,
        -- 이전 페이지 시작 시간을 표시, partition by 로 각 user_pseudo_id별로 그룹화함
        lag(event_time) over(partition by user_pseudo_id, ga_session_id order by event_time) as before_event_time
    from session_sort
),

session_count as (
    select 
        *,
       date_diff(event_time, before_event_time, second) as residence_time
    from session_lead 
    order by user_pseudo_id, event_time
),

group_page as (
select
    min(event_time) as start_time,
    user_pseudo_id,
    user_id,
    ga_session_id,
    page_location,
    sum(residence_time) as residence_sec
from session_count
group by user_pseudo_id,user_id, ga_session_id, page_location
order by user_pseudo_id, start_time
)

select * from group_page