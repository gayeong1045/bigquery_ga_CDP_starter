-- depends_on: {{ ref('stg_maderi_accounts') }}

{{
    config(
        materialized='table'
    )
}}

select
    *
from `maderi-cdp.dbt_ga.temp`