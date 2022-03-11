with inflow as (
    select
        event_time,
        user_pseudo_id,
        user_id,
        concat(traffic_source_site,'/',traffic_source_medium) as traffic_source_site,
    from {{ref('flat_ga_traffic')}}
    where event_name ='first_visit'
),

user_id_vlookup as(
    select 
        *
    from inflow a left join {{ref('inter_ga_useridmatching')}} b
        on a.user_pseudo_id = b.match_user_pseudo_id
),

inflow_conversion as (
    select 
        cast(event_time as date) as today,
        user_pseudo_id,
        match_user_id,
        traffic_source_site,
        case 
            when match_user_id is null then 0
            else 1
        end as is_joined,
        case 
            when match_user_id in (select user_id from {{ref('stg_maderi_paymenthistory')}}) then 1
            else 0
        end as is_purchase
    from user_id_vlookup
)


select * from inflow_conversion