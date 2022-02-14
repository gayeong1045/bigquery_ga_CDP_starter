-- account 테이블과 payment 테이블의 join. company의 결제내역 확인 가능
select
    sa.user_id,
    sa.accounts_user_is_active,
    sa.accounts_profile_company,
    sa.accounts_profile_license,
    ph.t_payment_history_event_at,
    ph.t_payment_history_amount
from {{ ref('stg_accounts') }} sa left join {{ ref('stg_payment_history') }} ph on sa.user_id = ph.user_id