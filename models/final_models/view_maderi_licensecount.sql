-- depends_on: {{ ref('stg_maderi_accounts') }}

{{
    config(
        materialized='incremental'
    )
}}

select
    *
from {{ref('cal_accounts_activeuser')}}
{% if is_incremental() %}

where today = (select max(cast(synced_time as date)) from {{ref('stg_maderi_accounts')}})

{% endif %}

