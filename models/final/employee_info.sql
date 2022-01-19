SELECT 
    user_id,
    accounts_user_id, 
    accounts_user_email, 
    accounts_profile_company, 
    accounts_profile_team, 
    accounts_profile_position, 
    accounts_user_first_name, 
    accounts_user_last_name, 
    accounts_profile_license, 
    accounts_user_is_active, 
    accounts_user_date_joined
FROM {{ref('stg_accounts')}}
WHERE accounts_user_email like '%datamarketing.co.kr'or accounts_profile_company = '데이터마케팅코리아' or accounts_profile_company = '데마코' or accounts_profile_company LIKE 'DMK%'