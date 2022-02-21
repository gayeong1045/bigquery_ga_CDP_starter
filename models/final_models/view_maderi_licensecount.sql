{{
    config(
        materialized='incremental',
        unique_key='today'
    )
}}

select
    *
from {{ref('cal_accounts_activeuser')}}
{% if is_incremental() %}

where today >= (select max(cast(synced_date as date)) from {{ref('stg_maderi_accounts')}})

{% endif %}

