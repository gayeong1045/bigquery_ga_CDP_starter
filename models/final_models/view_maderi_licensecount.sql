-- 라이센스 내역, 집계 테이블
{{
    config(
        materialized='incremental'
    )
}}

select
    current_date() as date,
    accounts_profile_license,
    count(accounts_profile_license) as count_license
from {{ ref('stg_maderi_accounts') }}
group by accounts_profile_license


