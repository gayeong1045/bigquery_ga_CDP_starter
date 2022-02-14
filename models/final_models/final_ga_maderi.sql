select 
    a.event_id,
    a.user_pseudo_id,
    a.user_id,
    a.event_date,
    a.event_time,
    a.event_name,
    a.ga_session_id,
    a.engagement_time_msec,
    a.percent_scrolled,
    a.page_location,
    a.page_referrer,
    a.traffic_source_name,
    a.traffic_source_medium,
    a.traffic_source_site,
    a.device_category,
    a.device_operating_system,
    a.device_web_info_browser,
    a.geo_country,
    a.geo_region,
    a.geo_city,
    a.match_user_id,
    a.is_employee,
    b.accounts_user_id,
    b.accounts_user_password,
    b.accounts_user_last_login,
    b.accounts_user_email,
    b.accounts_user_first_name,
    b.accounts_user_last_name,
    b.accounts_user_is_active,
    b.accounts_user_is_staff,
    b.accounts_user_is_admin,
    b.accounts_user_is_superuser,
    b.accounts_user_date_joined,
    b.accounts_user_created_at,
    b.accounts_user_updated_at,
    b.accounts_user_sns_type,
    b.accounts_profile_id,
    b.accounts_profile_company,
    b.accounts_profile_team,
    b.accounts_profile_position,
    b.accounts_profile_category,
    b.accounts_profile_license,
    b.accounts_profile_phone_number,
    b.accounts_profile_marketing_info,
    b.accounts_profile_private_info,
    b.accounts_profile_service_info,
    b.accounts_profile_level,
    c.t_payment_user_license_start_date,
    c.t_payment_user_license_end_date,
    c.t_payment_user_license_status,
    d.t_user_keyword_keyword_name as t_user_keyword_name,
    d.t_user_keyword_synonym,
    d.t_user_keyword_exclusion_word,
    d.t_user_keyword_created_at,
    d.t_user_keyword_is_own,
    e.t_ch_user_data_ch_id,
    e.t_ch_user_data_is_own,
    e.t_ch_user_data_sns_type,
    e.t_ch_user_data_created_at,
    f.t_payment_license_table_price as t_payment_license_price,
    f.t_payment_license_table_name as t_payment_license_name,
    g.accounts_loginhistory_created_at,
    h.t_payment_history_event_at,
    h.t_payment_history_amount,
    h.t_payment_history_name,
    h.t_payment_history_status
from {{ref('final_ga')}} a  full outer join {{ref('stg_accounts')}} b
                                on a.match_user_id = b.user_id
                            left join {{ref('stg_payment_user_license')}} c 
                                on b.user_id = c.user_id
                            left join {{ref('stg_user_keyword')}} d
                                on b.user_id = d.user_id
                            left join {{ref('stg_user_ch')}} e 
                                on b.user_id = e.user_id
                            left join {{ref('stg_payment_license_info')}} f 
                                on b.accounts_profile_license = f.t_payment_license_table_license
                            left join {{ref('stg_accounts_history')}} g 
                                on b.user_id = g.user_id
                            left join {{ref('stg_payment_history')}} h 
                                on b.user_id = h.user_id