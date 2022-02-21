{{
    config(
        materialized='table'
    )
}}

with non_flat as (
    select 
        event_id,
        user_pseudo_id,
        user_id,
        event_date,
        timestamp_micros(event_timestamp) as event_time
    from {{ref('stg_ga')}}
),

event_info as (
    select 
        event_id,
        event_name,
        max(if(event_params_key = 'ga_session_id', event_params_value_int, null)) as ga_session_id,
        max(if(event_params_key = 'engagement_time_msec', event_params_value_int, null)) as engagement_time_msec,
        max(if(event_params_key = 'percent_scrolled', event_params_value_int, null)) as percent_scrolled,        
        max(if(event_params_key = 'page_location', event_params_value_string, null)) as page_location,
        max(if(event_params_key = 'page_referrer', event_params_value_string, null)) as page_referrer,
        max(if(event_params_key = 'term', event_params_value_string, null)) as term
    from {{ ref('flat_ga_events') }}
    group by event_id, event_name
),

fin_ga as (
    select 
        a.event_id,
        a.user_pseudo_id,
        a.user_id,
        a.event_date,
        a.event_time,
        b.event_name,     
        b.ga_session_id,
        b.engagement_time_msec,
        b.percent_scrolled,        
        b.page_location,
        b.page_referrer,
        b.term,
        c.traffic_source_name,
        c.traffic_source_medium,
        c.traffic_source_site,
        d.device_category,
        d.device_operating_system,
        d.device_web_info_browser,
        e.geo_country,
        e.geo_region,
        e.geo_city
    from non_flat a left join event_info b 
                        on a.event_id = b.event_id 
                    left join {{ref('flat_ga_traffic')}} c
                        on a.event_id = c.event_id
                    left join {{ref('flat_ga_device')}} d
                        on a.event_id = d.event_id
                    left join {{ref('flat_ga_geo')}} e
                        on a.event_id = e.event_id
),

-- user_pseudo_id vlookup
user_id_vlookup as(
    select 
        *
    from fin_ga a left join {{ref('inter_ga_useridmatching')}} b
        on a.user_pseudo_id = b.match_user_pseudo_id
),

-- 직원 정보 태깅
tagging_employee as (
    select 
        *,
        case
            when match_user_id in (select user_id from {{ref('cal_accounts_loginsegment')}}) then 'employee'
            else 'customer'
        end as is_employee
    from user_id_vlookup 
)


select * from tagging_employee
