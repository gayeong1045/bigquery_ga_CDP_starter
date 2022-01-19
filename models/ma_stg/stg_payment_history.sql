{{ config(
    materialized='table'
)}}

with t_payment_history as (
    select
        id as t_payment_history_id,
        customer_uid as t_payment_history_customer_uid,
        merchant_uid as t_payment_history_merchant_uid,
        imp_uid as t_payment_history_imp_uid,
        custom_data as t_payment_history_custom_data,
        channel as t_payment_history_channel,
        event_at as t_payment_history_event_at,
        amount as t_payment_history_amount,
        name as t_payment_history_name,
        status as t_payment_history_status,
        buyer_name as t_payment_history_buyer_name,
        buyer_email as t_payment_history_buyer_email,
        buyer_tel as t_payment_history_buyer_tel,
        receipt_url as t_payment_history_receipt_url,
        msg as t_payment_history_msg,
        user_id as t_payment_history_user_id, -- 마대리 db 내 join key
        apply_num as t_payment_history_apply_num,
        card_code as t_payment_history_card_code,
        card_name as t_payment_history_card_name,
        card_number as t_payment_history_card_number,
        card_type as t_payment_history_card_type
    from {{source('maderi_db', 't_payment_history')}}
),

account_payment_history as (
    select
        user_id,
        t_payment_history_user_id, -- 마대리 db 내 join key
        t_payment_history_id,
        t_payment_history_customer_uid,
        t_payment_history_merchant_uid,
        t_payment_history_imp_uid,
        t_payment_history_custom_data,
        t_payment_history_channel,
        t_payment_history_event_at,
        t_payment_history_amount,
        t_payment_history_name,
        t_payment_history_status,
        t_payment_history_buyer_name,
        t_payment_history_buyer_email,
        t_payment_history_buyer_tel,
        t_payment_history_receipt_url,
        t_payment_history_msg,
        t_payment_history_apply_num,
        t_payment_history_card_code,
        t_payment_history_card_name,
        t_payment_history_card_number,
        t_payment_history_card_type
    from t_payment_history left join {{ ref('stg_accounts') }}
        on t_payment_history_user_id = accounts_user_id
)

select * from account_payment_history
