-- 매일 최근 활성 계정수를 count, trial 계정의 활성 기준은 회원가입시부터 15일 미만인자 
with trial_is_active as (
    select
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
        license,
        user_id,
        is_active
    from trial_is_active
    union all
    select 
        license,
        user_id,
        is_active
    from unique_userlicense
)

select 
    a.user_id,
    a.license,
    a.is_active,
    b.accounts_profile_marketing_info as marketing_info,
    b.accounts_user_created_at as join_date,
    b.accounts_user_first_name as first_name,
    b.accounts_user_last_name as last_name
from all_is_active a left join {{ref('stg_maderi_accounts')}} b
    on a.user_id = b.user_id