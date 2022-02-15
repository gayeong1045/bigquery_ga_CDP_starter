-- account 테이블과 payment 테이블의 join. company의 결제내역 확인 가능
{{
    config(
        materialized='table'
    )
}}

select
    sa.user_id,
    sa.accounts_user_is_active,
    sa.accounts_profile_company,
    sa.accounts_profile_license,
    ph.t_payment_history_event_at,
    ph.t_payment_history_amount,
    CASE
        when sa.user_id in (select user_id from {{ ref('inter_accounts_employeeinfo') }}) then 'employee'
        else 'customer'
    END AS is_employee
from {{ ref('stg_maderi_accounts') }} sa left join {{ ref('stg_maderi_paymenthistory') }} ph on sa.user_id = ph.user_id