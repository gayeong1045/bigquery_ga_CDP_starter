-- incremental로 증분 추출/분석 

{{ config(
    materialized='incremental'
)}}

-- vender_id as hostname은 자사 정보만 이용할 경우 삭제 가능

SELECT
  event_id,
  device.category as device_category,
  device.mobile_brand_name as device_mobile_brand_name,
  device.mobile_model_name as device_mobile_model_name,
  device.mobile_marketing_name as device_mobile_marketing_name,
  device.mobile_os_hardware_model as device_mobile_os_hardware_model,
  device.operating_system as device_operating_system,
  device.operating_system_version as device_operating_system_version,
  device.vendor_id as device_vendor_id,
  device.advertising_id as device_advertising_id,
  device.language as device_language,
  device.is_limited_ad_tracking as device_is_limited_ad_tracking,
  device.time_zone_offset_seconds as device_time_zone_offset_seconds,
  device.web_info.browser as device_web_info_browser,
  device.web_info.browser_version as device_web_info_browser_version,
  device.web_info.hostname as device_web_info_hostname

from {{ref('stg_events_customized')}}
			
	
    			
