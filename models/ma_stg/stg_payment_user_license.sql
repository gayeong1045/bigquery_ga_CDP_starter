{{ config(

    materialized='table'

)}}


with t_payment_user_license as (

    select

        id as t_payment_user_license_id,

        _type as t_payment_user_license__type,

        start_date as t_payment_user_license_start_date,

        end_date as t_payment_user_license_end_date,

        created_at as t_payment_user_license_created_at,

        status as t_payment_user_license_status,

        user_id as t_payment_user_license_user_id,

        imp_uid as t_payment_user_license_imp_uid,

        license as t_payment_user_license_license,

    from {{source('maderi_db', 't_payment_user_license')}}

),



account_payment_user_license as (

    select

        user_id,

        t_payment_user_license_user_id, -- 마대리 db 내 join key

        t_payment_user_license_id,

        t_payment_user_license__type,

        t_payment_user_license_start_date,

        t_payment_user_license_end_date,

        t_payment_user_license_created_at,

        t_payment_user_license_status,

        t_payment_user_license_imp_uid,

        t_payment_user_license_license

    from  t_payment_user_license left join {{ ref('stg_accounts') }}

        on t_payment_user_license_user_id = accounts_user_id

)



select * from account_payment_user_license