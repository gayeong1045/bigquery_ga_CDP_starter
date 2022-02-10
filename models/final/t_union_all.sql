with union_all as (
    select
        ga.event_time as TIME,                  -- TIME column으로 차후 모든 테이블의 time 값을 한 column에 묶어줌
                                                    -- ga의 evnet_time은 TIMESTAMP 자료형
        ga.match_user_id as userID,             -- ga 데이터의 user_id와 구분을 위해 userID로 column명 변경
        sa.accounts_user_id, 
        sa.accounts_user_password,
        sa.accounts_user_last_login,
        sa.accounts_user_email,
        sa.accounts_user_first_name,
        sa.accounts_user_last_name,
        sa.accounts_user_is_active,
        sa.accounts_user_is_staff,
        sa.accounts_user_is_admin,
        sa.accounts_user_is_superuser,
        sa.accounts_user_date_joined,
        sa.accounts_user_created_at,
        sa.accounts_user_updated_at,
        sa.accounts_user_sns_type,
        sa.accounts_profile_id,
        sa.accounts_profile_company,
        sa.accounts_profile_team,
        sa.accounts_profile_position,
        sa.accounts_profile_category,
        sa.accounts_profile_created_at,
        sa.accounts_profile_updated_at,
        sa.accounts_profile_license,
        sa.accounts_profile_phone_number,
        sa.accounts_profile_marketing_info,
        sa.accounts_profile_private_info,
        sa.accounts_profile_service_info,
        sa.accounts_profile_level,
        ga.event_id,
        ga.user_pseudo_id,
        ga.user_id,
        ga.event_date,
        ga.event_name,     
        ga.ga_session_id,
        ga.engagement_time_msec,
        ga.percent_scrolled,        
        ga.page_location,
        ga.page_referrer,
        ga.traffic_source_name,
        ga.traffic_source_medium,
        ga.traffic_source_site,
        ga.device_category,
        ga.device_operating_system,
        ga.device_web_info_browser,
        ga.geo_country,
        ga.geo_region,
        ga.geo_city,
        NULL as t_user_keyword_name,            -- ga 데이터가 아닌 column은 NULL 처리 후 alias로 column명 지정
        NULL as t_user_keyword_synonym,
        NULL as t_user_keyword_exclusion_word,
        NULL as t_user_keyword_is_own,
        NULL as t_ch_user_data_ch_id,
        NULL as t_ch_user_data_is_own,
        NULL as t_ch_user_data_sns_type,
        NULL as t_payment_history_amount,
        NULL as t_payment_history_name,
        NULL as t_payment_history_status,
        CASE                                    -- user_id를 통해 직원/고객 구분
            when ga.match_user_id in (select user_id from {{ ref('employee_info') }}) then 'employee'
            else 'customer'
        END AS is_employee,
        CASE                                    -- 마지막 column은 table 구분자로 table 상의 데이터가 어떤 개별적인 table에서 추출됐는지 구별
            WHEN ga.event_time IS NOT NULL
            THEN 'GA'
        END AS discriminator
    from {{ ref('final_ga')}} ga left join {{ ref('stg_accounts') }} sa on ga.match_user_id = sa.user_id

    UNION ALL

    select
        a.t_user_keyword_created_at,            -- t_user_keyword의 time값은 TIMESTAMP 자료형
        a.user_id,
        sa.accounts_user_id, 
        sa.accounts_user_password,
        sa.accounts_user_last_login,
        sa.accounts_user_email,
        sa.accounts_user_first_name,
        sa.accounts_user_last_name,
        sa.accounts_user_is_active,
        sa.accounts_user_is_staff,
        sa.accounts_user_is_admin,
        sa.accounts_user_is_superuser,
        sa.accounts_user_date_joined,
        sa.accounts_user_created_at,
        sa.accounts_user_updated_at,
        sa.accounts_user_sns_type,
        sa.accounts_profile_id,
        sa.accounts_profile_company,
        sa.accounts_profile_team,
        sa.accounts_profile_position,
        sa.accounts_profile_category,
        sa.accounts_profile_created_at,
        sa.accounts_profile_updated_at,
        sa.accounts_profile_license,
        sa.accounts_profile_phone_number,
        sa.accounts_profile_marketing_info,
        sa.accounts_profile_private_info,
        sa.accounts_profile_service_info,
        sa.accounts_profile_level, 
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,  
        a.t_user_keyword_keyword_name,
        a.t_user_keyword_synonym,
        a.t_user_keyword_exclusion_word,
        a.t_user_keyword_is_own,
        NULL, 
        NULL, 
        'keyword',
        NULL,
        NULL,
        NULL,
        CASE
            when a.user_id in (select user_id from {{ ref('employee_info') }}) then 'employee'
            else 'customer'
        END,
        CASE 
            WHEN a.t_user_keyword_created_at IS NOT NULL
            THEN 't_user_keyword'
        END 
    from {{ref('stg_user_keyword')}} a left join {{ ref('stg_accounts') }} sa on a.user_id = sa.user_id

    UNION ALL

    select
        cast(b.t_ch_user_data_created_at as timestamp),     -- t_ch_user_data의 time은 DATETIME 자료형으로 TIMESTAMP로 형변환
        b.user_id,                                              -- UNION하는 table의 time값의 대다수가 TIMESTAMP 자료형이므로 현재는 TIMESTAMP로 통일
        sa.accounts_user_id, 
        sa.accounts_user_password,
        sa.accounts_user_last_login,
        sa.accounts_user_email,
        sa.accounts_user_first_name,
        sa.accounts_user_last_name,
        sa.accounts_user_is_active,
        sa.accounts_user_is_staff,
        sa.accounts_user_is_admin,
        sa.accounts_user_is_superuser,
        sa.accounts_user_date_joined,
        sa.accounts_user_created_at,
        sa.accounts_user_updated_at,
        sa.accounts_user_sns_type,
        sa.accounts_profile_id,
        sa.accounts_profile_company,
        sa.accounts_profile_team,
        sa.accounts_profile_position,
        sa.accounts_profile_category,
        sa.accounts_profile_created_at,
        sa.accounts_profile_updated_at,
        sa.accounts_profile_license,
        sa.accounts_profile_phone_number,
        sa.accounts_profile_marketing_info,
        sa.accounts_profile_private_info,
        sa.accounts_profile_service_info,
        sa.accounts_profile_level,                                              
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        b.t_ch_user_data_ch_id,
        b.t_ch_user_data_is_own,
        b.t_ch_user_data_sns_type,
        NULL,
        NULL,
        NULL,
        CASE
            when b.user_id in (select user_id from {{ ref('employee_info') }}) then 'employee'
            else 'customer'
        END,
        CASE 
            WHEN b.t_ch_user_data_created_at IS NOT NULL
            THEN 't_ch_user_data' 
        END 
    from {{ref('stg_user_ch')}} b left join {{ ref('stg_accounts') }} sa on b.user_id = sa.user_id

    UNION ALL
        
    select
        c.accounts_loginhistory_created_at,             -- accounts_loginhistory의 time값은 TIMESTAMP 자료형
        c.user_id,
        sa.accounts_user_id, 
        sa.accounts_user_password,
        sa.accounts_user_last_login,
        sa.accounts_user_email,
        sa.accounts_user_first_name,
        sa.accounts_user_last_name,
        sa.accounts_user_is_active,
        sa.accounts_user_is_staff,
        sa.accounts_user_is_admin,
        sa.accounts_user_is_superuser,
        sa.accounts_user_date_joined,
        sa.accounts_user_created_at,
        sa.accounts_user_updated_at,
        sa.accounts_user_sns_type,
        sa.accounts_profile_id,
        sa.accounts_profile_company,
        sa.accounts_profile_team,
        sa.accounts_profile_position,
        sa.accounts_profile_category,
        sa.accounts_profile_created_at,
        sa.accounts_profile_updated_at,
        sa.accounts_profile_license,
        sa.accounts_profile_phone_number,
        sa.accounts_profile_marketing_info,
        sa.accounts_profile_private_info,
        sa.accounts_profile_service_info,
        sa.accounts_profile_level,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,  
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        CASE
            when c.user_id in (select user_id from {{ ref('employee_info') }}) then 'employee'
            else 'customer'
        END,
        CASE 
            WHEN c.accounts_loginhistory_created_at IS NOT NULL
            THEN 'accounts_loginhistory'
        END 
    from {{ ref('stg_accounts_history') }} c left join {{ ref('stg_accounts') }} sa on c.user_id = sa.user_id

    UNION ALL

    select
        cast(d.t_payment_history_event_at as timestamp),    -- t_payment_history_event_at은 STRING 자료형이므로 TIMESTAMP로 형변환
        d.user_id,
        sa.accounts_user_id, 
        sa.accounts_user_password,
        sa.accounts_user_last_login,
        sa.accounts_user_email,
        sa.accounts_user_first_name,
        sa.accounts_user_last_name,
        sa.accounts_user_is_active,
        sa.accounts_user_is_staff,
        sa.accounts_user_is_admin,
        sa.accounts_user_is_superuser,
        sa.accounts_user_date_joined,
        sa.accounts_user_created_at,
        sa.accounts_user_updated_at,
        sa.accounts_user_sns_type,
        sa.accounts_profile_id,
        sa.accounts_profile_company,
        sa.accounts_profile_team,
        sa.accounts_profile_position,
        sa.accounts_profile_category,
        sa.accounts_profile_created_at,
        sa.accounts_profile_updated_at,
        sa.accounts_profile_license,
        sa.accounts_profile_phone_number,
        sa.accounts_profile_marketing_info,
        sa.accounts_profile_private_info,
        sa.accounts_profile_service_info,
        sa.accounts_profile_level,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,  
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        d.t_payment_history_amount,
        d.t_payment_history_name,
        d.t_payment_history_status,
        CASE
            when d.user_id in (select user_id from {{ ref('employee_info') }}) then 'employee'
            else 'customer'
        END,
        CASE 
            WHEN d.t_payment_history_event_at IS NOT NULL
            THEN 't_payment_history'
        END 
    from {{ref('stg_payment_history')}} d left join {{ ref('stg_accounts') }} sa on d.user_id = sa.user_id
)

select *
from union_all ua left join {{ ref('loginhistory_evaluation') }} le
on ua.userID = le.userID_account
