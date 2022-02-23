{{ config(

    materialized='view'

)}}


with account as(
    select * from {{ ref('stg_maderi_accounts') }}
),

t_ch_user_data as (
    select
        ch_id as t_ch_user_data_ch_id,
        user_id as t_ch_user_data_user_id, -- 마대리 db 내 join key
        is_own as t_ch_user_data_is_own,
        sns_type as t_ch_user_data_sns_type,
        created_at as t_ch_user_data_created_at,
        updated_at as t_ch_user_data_updated_at
    from {{source('maderi_db', 't_ch_user_data')}}
),

union_website as (
    select
        t_ch_user_data_user_id, -- 마대리 db 내 join key
        t_ch_user_data_ch_id,
        t_ch_user_data_is_own,
        t_ch_user_data_sns_type,
        t_ch_user_data_created_at,
        t_ch_user_data_updated_at
    from t_ch_user_data 
    union all
    select 
        website_user_id,
        act_id,
        null,
        'website',
        created_at,
        updated_at
    from {{ref('stg_maderi_website')}}
),

account_ch_user_data as (
    select
        b.user_id,
        a.t_ch_user_data_user_id, -- 마대리 db 내 join key
        a.t_ch_user_data_ch_id,
        a.t_ch_user_data_is_own,
        a.t_ch_user_data_sns_type,
        a.t_ch_user_data_created_at,
        a.t_ch_user_data_updated_at
    from union_website a left join account b
        on a.t_ch_user_data_user_id = b.accounts_user_id
)


select * from account_ch_user_data