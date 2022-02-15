{{ config(

    materialized='view'

)}}



with account as(

    select * from {{ ref('stg_maderi_accounts') }}

),



t_user_keyword as (

    select

        id as t_user_keyword_id,

        keyword_name as t_user_keyword_keyword_name,

        synonym as t_user_keyword_synonym,

        exclusion_word as t_user_keyword_exclusion_word,

        created_at as t_user_keyword_created_at,

        updated_at as t_user_keyword_updated_at,

        user_id as t_user_keyword_user_id,  -- 마대리 db 내 join key

        is_own as t_user_keyword_is_own,

        rel_id as t_user_keyword_rel_id

    from {{source('maderi_db', 't_user_keyword')}}

),



account_user_keyword as (

    select

        user_id,

        t_user_keyword_user_id, -- 마대리 db 내 join key

        t_user_keyword_id,

        t_user_keyword_keyword_name,

        t_user_keyword_synonym,

        t_user_keyword_exclusion_word,

        t_user_keyword_created_at,

        t_user_keyword_updated_at,

        t_user_keyword_is_own,

        t_user_keyword_rel_id        

    from t_user_keyword left join account

        on t_user_keyword_user_id = accounts_user_id

)



select * from account_user_keyword