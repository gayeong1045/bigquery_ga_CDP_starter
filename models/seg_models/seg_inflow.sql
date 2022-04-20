{{
    config(
        materialized='table'
    )
}}

-- 유입 segment 분석

-- ga 이벤트 정보에 회원가입 일자를 붙여 회원가입 이전의 행동 데이터만 추출 
with event_info as (
    select 
        *,
        max(event_time) over(partition by user_pseudo_id) as last_time,
        min(event_time) over(partition by user_pseudo_id) as first_time   
    from 
        (select 
            a.event_id,
            a.user_pseudo_id,
            a.match_user_id,
            a.event_name,
            cast(a.event_time as datetime) as event_time,
            a.ga_session_id,
            a.is_employee,
            cast(b.accounts_user_created_at as datetime) as login_time,
        from {{ ref('view_trafficanalysis') }} a left join {{ ref('stg_maderi_accounts') }} b
            on a.match_user_id = b.user_id)
    where login_time > event_time or login_time is null
    order by user_pseudo_id
),

-- 페이지뷰수, 스크롤수, 클릭수 추출
count_event as (
    select 
        *,
        if(visit_period=0, event_cnt, event_cnt/visit_period) as daily_avg_cnt
        from
        (select
            user_pseudo_id,
            match_user_id,
            event_name,
            count(event_name) as event_cnt,
            max(last_time) as last_time,
            min(first_time) as first_time,
            date_diff(max(last_time),min(first_time),day) as visit_period
        from event_info
        group by user_pseudo_id, match_user_id, event_name)
    where event_name = 'page_view' or event_name = 'scroll' or event_name = 'click'
    order by user_pseudo_id
),


-- 행 값을 열 값으로 전환
trans_event as (
    select 
        user_pseudo_id,
        match_user_id,
        max(if(event_name = 'page_view', daily_avg_cnt, null)) as page_view,
        max(if(event_name = 'scroll', daily_avg_cnt, null)) as scroll,
        max(if(event_name = 'click', daily_avg_cnt, null)) as click,
    from count_event
    group by user_pseudo_id, match_user_id
    order by user_pseudo_id
),

-- 일평균 방문수 계산
visit_count as (
    select
        *,
        if(visit_period=0, visit_cnt, visit_cnt/visit_period) as visit 
    from 
        (select
            user_pseudo_id,
            match_user_id,
            count(distinct ga_session_id) as visit_cnt,
            date_diff(max(last_time),min(first_time),day) as visit_period
        from event_info 
        group by user_pseudo_id, match_user_id
        order by user_pseudo_id)
),

-- 일평균 체류시간(초) 계산
res_count as (
    select * from 
        (select 
            a.start_time,
            a.user_pseudo_id,
            a.match_user_id, 
            a.residence_sec,
            cast(b.accounts_user_created_at as datetime) as login_time
        from {{ ref('cal_ga_residence') }} a left join {{ ref('stg_maderi_accounts') }} b
            on a.match_user_id = b.user_id)
    where login_time > start_time or login_time is null
),

res_sum as (
    select 
        *,
        if(res_period=0, res_sec, res_sec/res_period) as residence
    from 
        (select
            user_pseudo_id,
            sum(residence_sec) res_sec,
            date_diff(max(start_time),min(start_time),day) as res_period
        from res_count
        group by user_pseudo_id)
    order by user_pseudo_id
),

seg_inflow as (
    select 
        a.user_pseudo_id,
        a.match_user_id,
        a.page_view,
        a.scroll,
        a.click,
        b.visit,
        c.residence
    from trans_event a 
            left join visit_count b on a.user_pseudo_id = b.user_pseudo_id 
            left join res_sum c on a.user_pseudo_id = c.user_pseudo_id 
),

--첫번째로 유입되었던 소스 및 키워드 추출
inflow_ch as (
    select
        user_pseudo_id,
        max(if(event_name = 'first_visit', traffic_source_site, null)) as traffic_site,
        max(term) as traffic_keyword
    from {{ref('view_trafficanalysis')}}
    group by user_pseudo_id
)

select 
    a.user_pseudo_id,
    a.match_user_id,
    a.page_view,
    a.scroll,
    a.click,
    a.visit,
    a.residence,
    b.traffic_site,
    b.traffic_keyword
from seg_inflow a left join inflow_ch b
    on a.user_pseudo_id = b.user_pseudo_id