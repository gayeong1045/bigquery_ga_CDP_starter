{{ config(
    materialized='view'
)}}

-- 등록 채널 명이 들어있는 meta 테이블 5개(페이스북, 유튜브, 인스타, 블로그, ga) 통합 
select
    ch_id,
    category,
    ch_name,
    created_at,
    thumbnail,
    'fb' as check_type
from {{source('maderi_db', 't_fb_ch_meta')}}
union all 
select 
    ch_id,
    null,
    ch_name,
    created_at,
    thumbnail,
    'yt' as check_type
from {{source('maderi_db', 't_yt_ch_meta')}}
union all
select 
    ch_id,
    null,
    ch_name,
    created_at,
    thumbnail,
    'nb' as check_type
from {{source('maderi_db', 't_nblog_ch_meta')}}
union all
select 
    ch_id,
    category,
    ch_name,
    created_at,
    thumbnail,
    'in' as check_type
from {{source('maderi_db', 't_insta_ch_meta')}}    
union all
select 
    act_id,
    null,
    act_name,
    created_at,
    null,
    'wb' as check_type
from {{source('maderi_db', 't_ga_account_meta')}}