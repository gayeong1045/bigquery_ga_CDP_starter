-- 처음 로그인한 시점의 user id에 해당하는 user_pseudo_id의 리스트 
-- 하나의 user_pseudo_id에 두개 이상의 user_id가 매칭되는 것을 막기 위해 발생시간이 더 짧은 것만 남기고 삭제

with user_info as (
    select 
        user_pseudo_id, 
        user_id,
        timestamp_micros(event_timestamp) as event_time,
        concat(user_pseudo_id, ifnull(user_id," ")) as distinct_key
    from {{ref('stg_events_customized')}}
    order by user_pseudo_id
),

-- 유니크한 user_id와 user_pseudo_id 리스트
distinct_info as(
    select 
        distinct distinct_key 
    from user_info
),

-- 유니크한 user_id와 user_pseudo_id 리스트를 시간 순으로 순서 표시
distinct_user as (
    select 
        a.distinct_key,
        a.user_pseudo_id,
        a.user_id,
        a.event_time,
        row_number() over (partition by user_pseudo_id, user_id order by event_time) as row_num
    from user_info a right join distinct_info b on a.distinct_key = b.distinct_key
),

-- 유니크한 user_id와 user_pseudo_id 마다 가장 먼저 매칭된 정보만 뽑고 user_pseudo_id별로 순서 표시
distinct_fin as (
    select 
        *,
        row_number() over (partition by user_pseudo_id order by event_time) as row
    from distinct_user 
    where row_num = 1
    order by user_pseudo_id
),

-- 하나의 user_pseudo_id에 두개 이상의 user_id가 매칭되는 것을 막기 위해 발생시간이 더 짧은 것만 남기고 삭제
fin as (
    select 
        user_pseudo_id,
        user_id,
        row
    from distinct_fin 
    where row = 1 or row = 2
)

select 
    user_pseudo_id as match_user_pseudo_id,
    string_agg(user_id, "") as match_user_id
from fin 
group by user_pseudo_id