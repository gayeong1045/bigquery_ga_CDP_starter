-- incremental, table sufix 등 학습 후 추가 

{{ config(
    materialized='table'
)}}

-- vender_id, hostname은 자사 정보만 이용할 경우 삭제 가능

SELECT
  event_id,
  device.category,
  device.mobile_brand_name,
  device.mobile_model_name,
  device.mobile_marketing_name,
  device.mobile_os_hardware_model,
  device.operating_system,
  device.operating_system_version,
  device.vendor_id,
  device.advertising_id,
  device.language,
  device.is_limited_ad_tracking,
  device.time_zone_offset_seconds,
  device.browser,
  device.browser_version,
  device.web_info.browser,
  device.web_info.browser_version,
  device.web_info.hostname

from {{ref('stg_events_customized')}}
			
	
    			
