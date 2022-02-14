-- ga를 통한 날짜별 방문자 user_pseudo_id
with visitor_per_day as (
    select
        extract(DATE from event_time) as event_time,
        user_pseudo_id
    from {{ ref('final_ga') }}
    group by event_time, user_pseudo_id
)

select * from visitor_per_day order by event_time