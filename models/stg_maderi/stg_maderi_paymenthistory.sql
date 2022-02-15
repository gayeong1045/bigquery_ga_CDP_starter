{{ config(
    materialized='view'
)}}

with t_payment_history as (
    select
        id as t_payment_history_id,
        customer_uid as user_id,
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
)

select * from t_payment_history
