{{
    config(
        materialized='table'
    )
}}

with event_info as (
    select 
        event_id,
        user_pseudo_id,
        match_user_id,
        event_name,
        cast(event_time as datetime) as event_time,
        ga_session_id,
        is_employee
    from {{ ref('view_trafficanalysis') }}
)

select * from event_info
