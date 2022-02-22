-- 매일 최근 활성 계정수를 count, trial 계정의 활성 기준은 회원가입시부터 15일 미만인자 
with trial_is_active as (
    select
        cast(synced_time as date) as today,
        user_id,
        cast(accounts_profile_created_at as date) as joined_date,
        accounts_profile_license as license,
        case 
            when FORMAT_DATE("%Y-%m-%d", DATE_ADD(cast(accounts_profile_created_at as date), INTERVAL 15 DAY)) < FORMAT_DATE("%Y-%m-%d", CURRENT_DATE()) then 'expire'
            else 'active'
        end as is_active
    from {{ref('stg_maderi_accounts')}}
    where accounts_profile_license = 'NP0000'
    order by 1
),
-- 매일 최근 활성 계정수를 count, 나머지 계정의 활성 기준은 payment_user_license 테이블의 status가 active인자
etc_is_active as(
    select 
        cast(a.synced_time as date) as today,
        a.user_id,
        a.accounts_profile_license as license,
        b.t_payment_user_license_status as status,
        case 
            when b.t_payment_user_license_status = 'active' then 'active'
            else 'expire' 
        end as is_active,
        b.t_payment_user_license_created_at as license_create,
        lead(a.user_id) over(order by a.user_id, b.t_payment_user_license_created_at) as before_user_id
    from {{ref('stg_maderi_accounts')}} a left join {{ref('stg_maderi_paymentuserlicense')}} b
        on a.user_id = b.user_id
    where a.accounts_profile_license != 'NP0000'
    order by user_id, license_create desc

),

unique_userlicense as (
    select * from 
        (
        select
            today,
            license,
            user_id,
            is_active,
            license_create,
            before_user_id,
            case
                when user_id = before_user_id then 0
                else 1
            end as unique_user
        from etc_is_active)
    where unique_user = 1
),

all_is_active as (
    select 
        today,
        license,
        user_id,
        is_active
    from trial_is_active
    union all
    select 
        today,
        license,
        user_id,
        is_active
    from unique_userlicense
),

active_tag as (
    select
        today,
        user_id,
        concat(is_active, '_', license) as status_license
    from all_is_active
    order by today
),

active_count as (
    select distinct
        status_license,
        count(distinct user_id) as daily_active_users
    from active_tag
    group by 1
    order by 1
),

daily_count as (
    select 
        status_license,
        sum(daily_active_users) as daily_user_count
    from active_count
    group by status_license
)

select b.today, a.status_license, a.daily_user_count from daily_count a
cross join (select max(cast(synced_time as date)) as today from {{ref('stg_maderi_accounts')}}) b
