with event_info as (
    select 
        event_time,
        event_name,
        user_pseudo_id,
        user_id,
        max(if(event_params_key = 'ga_session_id', event_params_value_int, null)) as ga_session_id,  
        max(if(event_params_key = 'page_location', event_params_value_string, null)) as page_location,
    from {{ ref('flat_ga_events') }}
    group by event_time, event_name, user_pseudo_id, user_id
), 

session_sort as (
    select distinct
        event_time,
        page_location,
        user_pseudo_id,
        ga_session_id
    from event_info
),

session_lead as (
    select 
        *,
        lag(event_time) over(order by user_pseudo_id, event_time) as before_event_time
    from session_sort
),

session_count as (
    select 
        *,
        event_time - before_event_time as residence_time
    from session_lead
    order by user_pseudo_id, event_time
)

select * from session_count
order by user_pseudo_id, event_time

