-- 매일 최근 활성 계정수를 count 
with sync_count as (
    select
        cast(synced_time as date) as today,
        count(distinct user_id) as daily_active_users

    from {{ref('stg_maderi_accounts')}}
    where accounts_user_is_active = true

    group by 1
    order by 1
),

daily_count as (
    select 
        max(today) as today,
        sum(daily_active_users) as daily_active_users
    from sync_count
)

select * from daily_count
