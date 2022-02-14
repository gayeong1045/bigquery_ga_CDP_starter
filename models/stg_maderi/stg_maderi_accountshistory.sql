{{ config(
    materialized='table'
)}}

with accounts_loginhistory as (
    select 
        id as accounts_loginhistory_id,
        username as user_id,
        created_at as accounts_loginhistory_created_at,
        data as accounts_loginhistory_data
    from {{source('maderi_db', 'accounts_testloginhistory')}}

)

select * from accounts_loginhistory