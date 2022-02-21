-- incremental로 증분 추출/분석 

{{ config(
    materialized='view'
)}}


SELECT
  event_id,
  -- privacy info
  privacy_info.analytics_storage as privacy_info_analytics_storage,
  privacy_info.ads_storage as privacy_info_ads_storage,
  privacy_info.uses_transient_token as privacy_info_uses_transient_token,
  -- user_properties(로그인한 고객정보 as user-id를 str 키로 가짐)
  user_properties.key as user_properties_key,
  user_properties.value.string_value as user_properties_value_string_value,
  user_properties.value.int_value as user_properties_value_int_value,
  user_properties.value.float_value as user_properties_value_float_value,
  user_properties.value.double_value as user_properties_value_double_value,
  -- user_ltv
  user_ltv.revenue as user_ltv_revenue,
  user_ltv.currency as user_ltv_currency,
  -- app_info
  app_info.id as app_info_id,
  app_info.version as app_info_version,
  app_info.install_store as app_info_install_store,
  app_info.firebase_app_id as app_info_firebase_app_id,
  app_info.install_source as  app_info_install_source,
  -- event_dimensions
  event_dimensions.hostname as event_dimensions_hostname
  -- ecommerce 생략
  -- item 은 비어있으므로 생략 
  -- 추가 필요시 참고) https://www.ga4bigquery.com/tutorial-how-to-flatten-the-ga4-bigquery-export-schema-for-relational-databases-using-unnest/ 

from {{ref('stg_ga')}},
UNNEST(user_properties) AS user_properties

