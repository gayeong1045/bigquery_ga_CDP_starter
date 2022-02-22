{{ config(

    materialized='view'

)}}

select 
        a.user_id as website_user_id, -- profile_id와 연결하기 위한 key
        a.profile_id,
        a.created_at,
        a.updated_at,
        b.act_id
from {{source('maderi_db', 't_ga3_user_data')}} a left join {{source('maderi_db', 't_ga3_profile_meta')}} b 
    on a.profile_id = b.profile_id
