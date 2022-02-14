{{ config(
    materialized='table'
)}}

with accounts_user as(
    select 
        id as accounts_user_id, -- 마대리 db 내 join key
        password as accounts_user_password,
        last_login as accounts_user_last_login,
        email as accounts_user_email,
        username as user_id,
        first_name as accounts_user_first_name,
        last_name as accounts_user_last_name,
        is_active as accounts_user_is_active,
        is_staff as accounts_user_is_staff,
        is_admin as accounts_user_is_admin,
        is_superuser as accounts_user_is_superuser,
        date_joined as accounts_user_date_joined,
        created_at as accounts_user_created_at,
        updated_at as accounts_user_updated_at,
        sns_type as accounts_user_sns_type
    from {{source('maderi_db', 'accounts_user')}}
),

accounts_profile as(
    select
        id as accounts_profile_id,
        company as accounts_profile_company,
        team as accounts_profile_team,
        position as accounts_profile_position,
        category as accounts_profile_category,
        created_at as accounts_profile_created_at,
        updated_at as accounts_profile_updated_at,
        user_id as accounts_profile_user_id, -- 마대리 db 내 join key
        license as accounts_profile_license,
        phone_number as accounts_profile_phone_number,
        marketing_info as accounts_profile_marketing_info,
        private_info as accounts_profile_private_info,
        service_info as accounts_profile_service_info,
        level as accounts_profile_level
    from {{source('maderi_db', 'accounts_profile')}}
),

accounts as (
    select * 
    from accounts_user left join accounts_profile
        on accounts_user_id = accounts_profile_user_id 
)

select * from accounts