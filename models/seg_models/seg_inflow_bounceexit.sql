{{
    config(
        materialized='table'
    )
}}
-- ga 데이터 중 식별화정보, 시간, 이벤트 정보, 세션, 페이지 위치정보만 가지고 온다
with flat_events as (
    select 
        user_pseudo_id,
        cast(event_time as datetime) as event_time,
        event_name,
        ga_session_id,
        page_location
    from {{ref('view_trafficanalysis')}}
    order by user_pseudo_id, event_time
),
-- 사용자마다 세션별 페이지별 이벤트 발생 개수 count
event_cnt as (
    select 
        user_pseudo_id,
        ga_session_id,
        page_location,
        min(event_time) as start_time,
        count(event_name) as event_cnt
    from flat_events
    group by user_pseudo_id, ga_session_id, page_location
    order by user_pseudo_id, ga_session_id, start_time desc
),

-- 사용자마다 세션별 종료 페이지 표시
-- 페이지 로케이션별 시작 시간을 desc했을 때 가장 첫번째값만 추출
exit_page as (
    select * from
        (select 
            user_pseudo_id,
            ga_session_id,
            page_location,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, ga_session_id ORDER BY user_pseudo_id, ga_session_id, start_time DESC) as row
        from event_cnt
        order by user_pseudo_id, ga_session_id)
    where row=1
),

-- 사용자마다 세션별 이탈했을 경우 이탈 페이지 표시
-- 단일 세션일 때의 페이지 추출함
bounce_page as(
    select * from 
        (select
            user_pseudo_id,
            ga_session_id,
            count(ga_session_id) as session_cnt,
            max(page_location) as bounce_page
        from event_cnt
        group by user_pseudo_id, ga_session_id
        order by user_pseudo_id, ga_session_id)
    where session_cnt=1
),

-- 사용자 세션별 종료페이지와 이탈페이지 취합
exit_bounce as (
    select
        cast(a.user_pseudo_id as string) as user_pseudo_id,
        cast(a.ga_session_id as string) as ga_session_id,
        a.page_location as exit_page,
        b.bounce_page
    from exit_page a left join bounce_page b
        on a.user_pseudo_id = b.user_pseudo_id and a.ga_session_id = b.ga_session_id
    order by user_pseudo_id, ga_session_id
),

-- key값 생성 및 추후 페이지 그룹핑
final as (
    select 
        concat(user_pseudo_id, '_', ga_session_id) as key, 
        * 
    from exit_bounce
)

select * from final