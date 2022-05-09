{{
    config(
        materialized='table'
    )
}}

-- 회원가입 segment 분석
with all_user as (
    select distinct
        user_id, 
        accounts_profile_created_at as login_time
    from {{ ref('stg_maderi_accounts') }}
),

-- 유저별 일평균 로그인 횟수 count
login_count as (
    select 
        user_id,
        login_count / date_diff(today,first_login_date, day) as daily_login_time
    from 
        (select
            user_id,
            count(accounts_loginhistory_created_at) as login_count,
            min(cast(accounts_loginhistory_created_at as date)) as first_login_date,
            cast(CURRENT_DATETIME() as date) as today
        from {{ ref ('stg_maderi_accountshistory') }}
        group by user_id)
    order by user_id
)

select * from login_count