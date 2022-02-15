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



account_ch_user_data as (

    select

        user_id,

        t_ch_user_data_user_id, -- 마대리 db 내 join key

        t_ch_user_data_ch_id,

        t_ch_user_data_is_own,

        t_ch_user_data_sns_type,

        t_ch_user_data_created_at,

        t_ch_user_data_updated_at

    from t_ch_user_data left join account

        on t_ch_user_data_user_id = accounts_user_id

)



select * from account_ch_user_data