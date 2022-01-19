with non_flat as (
    select 
        event_id,
        user_pseudo_id,
        user_id,
        event_date,
        timestamp_micros(event_timestamp) as event_time
    from {{ref('stg_events_customized')}}
),

event_info as (
    select 
        event_id,
        event_name,
        max(if(event_params_key = 'ga_session_id', event_params_value_int, null)) as ga_session_id,
        max(if(event_params_key = 'engagement_time_msec', event_params_value_int, null)) as engagement_time_msec,
        max(if(event_params_key = 'percent_scrolled', event_params_value_int, null)) as percent_scrolled,        
        max(if(event_params_key = 'page_location', event_params_value_string, null)) as page_location,
        max(if(event_params_key = 'page_referrer', event_params_value_string, null)) as page_referrer
    from {{ ref('flat_events') }}
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
                    left join {{ref('flat_traffic')}} c
                        on a.event_id = c.event_id
                    left join {{ref('flat_device')}} d
                        on a.event_id = d.event_id
                    left join {{ref('flat_geo')}} e
                        on a.event_id = e.event_id
)

select * from fin_ga