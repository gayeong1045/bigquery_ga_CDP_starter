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
            date_diff(max(last_time),min(first_time),day) as visit_period,
            max(is_employee) as is_employee
        from event_info
        group by user_pseudo_id, match_user_id, event_name)
    where event_name = 'page_view' or event_name = 'scroll' or event_name = 'test_btn_click_search'
    order by user_pseudo_id
),


-- 행 값을 열 값으로 전환
trans_event as (
    select 
        user_pseudo_id,
        match_user_id,
        max(if(event_name = 'page_view', daily_avg_cnt, null)) as page_view,
        max(if(event_name = 'scroll', daily_avg_cnt, null)) as scroll,
        max(if(event_name = 'test_btn_click_search', daily_avg_cnt, null)) as click,
        max(is_employee) as is_employee
    from count_event
    group by user_pseudo_id, match_user_id
    order by user_pseudo_id
),

-- 누적 방문수 계산 (방문수 = ga_session 수라고 가정)
visit_count as (
    select
        user_pseudo_id,
        match_user_id,
        count(distinct ga_session_id) as visit,
    from event_info 
    group by user_pseudo_id, match_user_id
    order by user_pseudo_id
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
        if(a.page_view is null, 0, a.page_view) as page_view,
        if(a.scroll is null, 0, a.scroll) as scroll,
        if(a.click is null, 0, a.click) as click,
        if(b.visit is null, 0, b.visit) as visit,
        if(c.residence is null, 0, c.residence) as residence,
        a.is_employee
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
),

before_normalize as (
    select 
        a.user_pseudo_id,
        a.match_user_id,
        a.page_view,
        a.scroll,
        a.click,
        a.visit,
        a.residence,
        b.traffic_site,
        b.traffic_keyword,
        a.is_employee
    from seg_inflow a left join inflow_ch b
        on a.user_pseudo_id = b.user_pseudo_id
),

after_normalize as (
    select
        user_pseudo_id,
        match_user_id,
        page_view,
        scroll,
        click,
        visit,
        residence,
        (page_view-min_page_view)/(max_page_view-min_page_view) as z_page_view,
        (scroll-min_scroll)/(max_scroll-min_scroll) as z_scroll,
        (click-min_click)/(max_click-min_click) as z_click,
        (visit-min_visit)/(max_visit-min_visit) as z_visit,
        (residence-min_residence)/(max_residence-min_residence) as z_residence,
        is_employee
    from 
    (-- 각 인자들의 최댓값과 최솟값을 컬럼으로 표시
        select * from before_normalize 
        cross join 
        (select
            max(page_view) as max_page_view,
            min(page_view) as min_page_view,
            max(scroll) as max_scroll,
            min(scroll) as min_scroll,
            max(click) as max_click,
            min(click) as min_click,
            max(visit) as max_visit,
            min(visit) as min_visit,
            max(residence) as max_residence,
            min(residence) as min_residence,
        from before_normalize)
    )
)

select * from after_normalize
