-- 유,무료 계정별 고객 segment.
-- 로그인 횟수로 평균 이상 및 미만 평가

{{ config(
    materialized='view'
)}}

-- accounts와 accounts_history의 join 
with accounts_license as (
    select
        user_id,
        accounts_profile_license as accounts_license,
        accounts_user_is_active as is_active
    from {{ ref('stg_maderi_accounts') }}
),

accounts_license_loginhistory as (
    select
        a.user_id,
        a.accounts_loginhistory_created_at,
        b.accounts_license,
        b.is_active
    from {{ ref('stg_maderi_accountshistory') }} a left join accounts_license b on a.user_id = b.user_id
),

-- trial 고객
count_loginhistory_trial as (  
    select
        user_id,
        accounts_license,
        count(accounts_loginhistory_created_at) as login_count,
        is_active
    from accounts_license_loginhistory
    WHERE
        accounts_license = 'NP0000'
    group by user_id, accounts_license, is_active
),

count_average_trial as (
    select
        avg(login_count) as average
    from count_loginhistory_trial
),

trial_count as (
    select
        user_id as userID_account,
        accounts_license,
        is_active,
        login_count,
        (select average from count_average_trial) as average_count,
        case
            when login_count >= (select average from count_average_trial) then '평균 이상'
            when login_count < (select average from count_average_trial) then '평균 미만' 
        end as evaluation,
        CASE
            when user_id in (select user_id from {{ ref('inter_accounts_employeeinfo') }}) then 'employee'
            else 'customer'
        END as is_employee,
        '무료회원' as discriminator_account
    from count_loginhistory_trial
    group by user_id, accounts_license, is_active, login_count
),

-- 유료 고객
count_loginhistory_paid as (  
    select
        user_id,
        accounts_license,
        is_active,
        count(accounts_loginhistory_created_at) as login_count
    from accounts_license_loginhistory
    WHERE
        accounts_license = 'CP1000' or
        accounts_license = 'CP2000' or
        accounts_license = 'CP3000' or
        accounts_license = 'SP1000' or
        accounts_license = 'SP2000'
    group by user_id, accounts_license, is_active
),

count_average_paid as (
    select
        avg(login_count) as average
    from count_loginhistory_paid
),

paid_count as (
    select
        user_id,
        accounts_license,
        is_active,
        login_count,
        (select average from count_average_paid) as average_count,
        case
            when login_count >= (select average from count_average_paid) then '평균 이상'
            when login_count < (select average from count_average_paid) then '평균 미만' 
        end as evaluation,
        CASE
            when user_id in (select user_id from {{ ref('inter_accounts_employeeinfo') }}) then 'employee'
            else 'customer'
        END as is_employee,
        '유료회원'
    from count_loginhistory_paid
    group by user_id, accounts_license, is_active, login_count
)

select * from trial_count where is_employee = 'customer'
--where a.userID_account not in (select b.user_id from {{ ref('inter_accounts_employeeinfo') }} b)

union all
select * from paid_count where is_employee = 'customer'
--where a.user_id not in (select b.user_id from {{ ref('inter_accounts_employeeinfo') }} b) 



