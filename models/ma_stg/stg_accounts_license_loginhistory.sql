-- loginhistory_evaluation을 위한 stg_accounts와 stg_accounts_history의 join
with accounts_license as (
    select
        user_id,
        accounts_profile_license as accounts_license
    from {{ ref('stg_accounts') }}
)

select
    a.user_id,
    a.accounts_loginhistory_created_at,
    b.accounts_license
from {{ ref('stg_accounts_history') }} a left join accounts_license b on a.user_id = b.user_id
