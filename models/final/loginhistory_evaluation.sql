-- 유,무료 계정별 고객 segment.
-- 로그인 횟수로 평균 이상 및 미만 평가
with count_loginhistory_trial as (  
    select
        user_id,
        accounts_license,
        count(accounts_loginhistory_created_at) as login_count
    from {{ ref('stg_accounts_license_loginhistory') }}
    WHERE
        accounts_license = 'NP0000'
    group by user_id, accounts_license
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
        login_count,
        (select average from count_average_trial) as average_count,
        case
            when login_count >= (select average from count_average_trial) then '평균 이상'
            when login_count < (select average from count_average_trial) then '평균 미만' 
        end as evaluation,
        '무료회원' as discriminator_account
    from count_loginhistory_trial
    group by user_id, accounts_license, login_count
),

count_loginhistory_paid as (  
    select
        user_id,
        accounts_license,
        count(accounts_loginhistory_created_at) as login_count
    from {{ ref('stg_accounts_license_loginhistory') }}
    WHERE
        accounts_license = 'CP1000' or
        accounts_license = 'CP2000' or
        accounts_license = 'CP3000' or
        accounts_license = 'SP1000' or
        accounts_license = 'SP2000'
    group by user_id, accounts_license
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
        login_count,
        (select average from count_average_paid) as average_count,
        case
            when login_count >= (select average from count_average_paid) then '평균 이상'
            when login_count < (select average from count_average_paid) then '평균 미만' 
        end as evaluation,
        '유료회원'
    from count_loginhistory_paid
    group by user_id, accounts_license, login_count
)

select * from trial_count 
union all
select * from paid_count

