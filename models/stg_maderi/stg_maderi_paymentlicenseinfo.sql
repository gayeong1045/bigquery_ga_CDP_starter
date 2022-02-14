{{ config(

    materialized='table'

)}}



with t_payment_license_table as (

    select

        id as t_payment_license_table_id,

        license as t_payment_license_table_license, -- 마대리 db 내 join key

        price as t_payment_license_table_price,

        reg_keyword as t_payment_license_table_reg_keyword,

        next_schedule as t_payment_license_table_next_schedule,

        retry_seconds as t_payment_license_table_retry_seconds,

        reg_ad_facebook as t_payment_license_table_reg_ad_facebook,

        reg_ad_google as t_payment_license_table_reg_ad_google,

        reg_ad_naver as t_payment_license_table_reg_ad_naver,

        reg_facebook as t_payment_license_table_reg_facebook,

        reg_instagram as t_payment_license_table_reg_instagram,

        reg_nblog as t_payment_license_table_reg_nblog,

        reg_trendteller as t_payment_license_table_reg_trendteller,

        reg_website as t_payment_license_table_reg_website,

        reg_youtube as t_payment_license_table_reg_youtube,

        is_comment as t_payment_license_table_is_comment,

        is_download as t_payment_license_table_is_download,

        name as t_payment_license_table_name

    from {{source('maderi_db', 't_payment_license_table')}}

)



select * from t_payment_license_table