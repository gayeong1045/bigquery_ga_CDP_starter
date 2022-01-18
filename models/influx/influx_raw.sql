with non_flat as (
    select 
        event_id,
        user_pseudo_id,
        user_id,
        event_date,
        timestamp_micros(event_timestamp) as event_time
    from {{ref('stg_events_customized')}}
where user_id is null 
),

event_info as (
    select 
        event_id,
        event_name,
        max(if(event_params_key = 'ga_session_id', event_params_value_int, null)) as ga_session_id,
        max(if(event_params_key = 'page_location', event_params_value_string, null)) as page_location,
        max(if(event_params_key = 'page_referrer', event_params_value_string, null)) as page_referrer
    from {{ ref('flat_events') }}
    where event_name ='first_visit'
    group by event_id, event_name

)

select * from event_info