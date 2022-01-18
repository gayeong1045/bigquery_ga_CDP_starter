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
        event_id
    from {{ ref('flat_events') }}
)

select * from event_info