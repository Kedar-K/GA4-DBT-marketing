create or replace table `burger-builder-39571.ga4_marketing.ga_first_visit` as

with base as(
SELECT 
  parse_date("%Y%m%d", event_date) date
  ,DATETIME(timestamp_micros(event_timestamp),"Asia/Kolkata") timestamp
  ,extract(hour from DATETIME(timestamp_micros(event_timestamp),"Asia/Kolkata")) hour
  ,user_pseudo_id pseudo_id
  ,timestamp_micros(user_first_touch_timestamp) first_touch_timestamp
  ,timestamp_micros(event_previous_timestamp) previous_timestamp
  ,user_id namshi_id

  -- event_params
  ,ep.key ep_key
  ,coalesce(ep.value.string_value,cast(ep.value.int_value as string),cast(ep.value.float_value as string),cast(ep.value.double_value as string) ) ep_value

  -- user_properties
  ,up.key up_key
  ,coalesce(cast(up.value.string_value as string),cast(up.value.int_value as string),cast(up.value.float_value as string),cast(up.value.double_value as string) ) up_value



  ,main.* except(event_params, event_date, event_timestamp, user_properties, user_pseudo_id, user_first_touch_timestamp,event_previous_timestamp, user_id)
FROM `burger-builder-39571.ga4_marketing.ga_partition_sessions_20210127` main
left join unnest(event_params) ep
left join unnest(user_properties) up
where event_key="2022-01-30"
)

-- final_select
select 
  date
  ,timestamp
  ,hour
  ,pseudo_id
  ,first_touch_timestamp
  ,previous_timestamp

  --traffic
  ,traffic_source.name traffic_name
  ,traffic_source.medium traffic_medium
  ,traffic_source.source traffic_source

  --device
  ,device.category category
  ,device.mobile_brand_name mobile_brand_name
  ,device.mobile_model_name mobile_model_name
  ,device.mobile_marketing_name mobile_marketing_name
  ,device.mobile_os_hardware_model mobile_os_hardware_model
  ,device.operating_system operating_system
  ,device.operating_system_version operating_system_version
  ,device.vendor_id vendor_id
  ,device.advertising_id advertising_id
  ,device.language language
  ,device.is_limited_ad_tracking is_limited_ad_tracking
  ,device.time_zone_offset_seconds time_zone_offset_seconds
  ,platform

  --app_info
  , app_info.id app_id
  ,app_info.version app_version
  ,app_info.install_store app_install_store
  ,app_info.firebase_app_id app_firebase_app_id
  ,app_info.install_source app_install_source
  
  --geo
  ,geo.country country
  ,geo.region region
  ,geo.city city
  ,geo.sub_continent sub_continent
  ,geo.metro metro

  -- privacy
  ,privacy_info.analytics_storage analytics_storage
  ,privacy_info.ads_storage ads_storage
  ,privacy_info.uses_transient_token uses_transient_token

  ,event_name
  ,event_bundle_sequence_id
  ,event_server_timestamp_offset

  --event_params

,MAX(IF(ep_key = "transactionSkusConfigStringCommaList",ep_value,NULL)) as transactionSkusConfigStringCommaList
,MAX(IF(ep_key = "aw_merchant_id",ep_value,NULL)) as aw_merchant_id
,MAX(IF(ep_key = "transaction_id",ep_value,NULL)) as transaction_id
,MAX(IF(ep_key = "transactionTotal",ep_value,NULL)) as transactionTotal
,MAX(IF(ep_key = "transactionTotal",ep_value,NULL)) as transactionTotal_str
,MAX(IF(ep_key = "transactionSkusConfigObjectUSD",ep_value,NULL)) as transactionSkusConfigObjectUSD
,MAX(IF(ep_key = "transactionCoupon",ep_value,NULL)) as transactionCoupon
,MAX(IF(ep_key = "ignore_referrer",ep_value,NULL)) as ignore_referrer
,MAX(IF(ep_key = "transactionShippingFees",ep_value,NULL)) as transactionShippingFees
,MAX(IF(ep_key = "page_location",ep_value,NULL)) as page_location
,MAX(IF(ep_key = "page_title",ep_value,NULL)) as page_title
,MAX(IF(ep_key = "medium",ep_value,NULL)) as medium
,MAX(IF(ep_key = "transactionCount",ep_value,NULL)) as transactionCount
,MAX(IF(ep_key = "aw_feed_language",ep_value,NULL)) as aw_feed_language
,MAX(IF(ep_key = "appVersion", ep_value, NULL)) as appVersion	
,MAX(IF(ep_key = "firebase_conversion", ep_value, NULL)) as firebase_conversion	
,MAX(IF(ep_key = "engagement_time_msec",ep_value,NULL)) as engagement_time_msec
,MAX(IF(ep_key = "shipping",ep_value,NULL)) as shipping
,MAX(IF(ep_key = "shipping",ep_value,NULL)) as shipping_str
,MAX(IF(ep_key = "value",ep_value,NULL)) as value
,MAX(IF(ep_key = "firebase_screen_id",ep_value,NULL)) as firebase_screen_id
,MAX(IF(ep_key = "item_list",ep_value,NULL)) as item_list
,MAX(IF(ep_key = "transactionPaymentMethod",ep_value,NULL)) as transactionPaymentMethod
,MAX(IF(ep_key = "transactionHasCoupon",ep_value,NULL)) as transactionHasCoupon
,MAX(IF(ep_key = "transactionCurrency",ep_value,NULL)) as transactionCurrency
,MAX(IF(ep_key = "sku", ep_value, NULL)) as sku	
,MAX(IF(ep_key = "transactionIsFirstOrder",ep_value,NULL)) as transactionIsFirstOrder
,MAX(IF(ep_key = "engaged_session_event",ep_value,NULL)) as engaged_session_event
,MAX(IF(ep_key = "campaign",ep_value,NULL)) as campaign
,MAX(IF(ep_key = "page_referrer",ep_value,NULL)) as page_referrer
,MAX(IF(ep_key = "transactionTotalUSD",ep_value,NULL)) as transactionTotalUSD
,MAX(IF(ep_key = "transactionTotalUSD",ep_value,NULL)) as transactionTotalUSD_str
,MAX(IF(ep_key = "transactionId",ep_value,NULL)) as transactionId
,MAX(IF(ep_key = "affiliation",ep_value,NULL)) as affiliation
,MAX(IF(ep_key = "tax",ep_value,NULL)) as tax
,MAX(IF(ep_key = "session_engaged",ep_value,NULL)) as session_engaged
,MAX(IF(ep_key = "userRemainingWalletBalance",ep_value,NULL)) as userRemainingWalletBalance
,MAX(IF(ep_key = "firebase_event_origin", ep_value, NULL)) as firebase_event_origin	
,MAX(IF(ep_key = "transactionCustomerSegment",ep_value,NULL)) as transactionCustomerSegment
,MAX(IF(ep_key = "source",ep_value,NULL)) as source
,MAX(IF(ep_key = "ga_session_id", ep_value, NULL)) as ga_session_id	
,MAX(IF(ep_key = "aw_feed_country",ep_value,NULL)) as aw_feed_country
,MAX(IF(ep_key = "firebase_screen_class",ep_value,NULL)) as firebase_screen_class
,MAX(IF(ep_key = "userHasWalletBalance",ep_value,NULL)) as userHasWalletBalance
,MAX(IF(ep_key = "currency",ep_value,NULL)) as currency
,MAX(IF(ep_key = "ga_session_number", ep_value, NULL)) as ga_session_number	
,MAX(IF(ep_key = "coupon",ep_value,NULL)) as coupon
,MAX(IF(ep_key = "transactionShippingFeesLocal",ep_value,NULL)) as transactionShippingFeesLocal
,MAX(IF(ep_key = "firebase_error", ep_value, NULL)) as firebase_error	

from base

group by 1, 2, 3, 4, 5, 6, 7 ,8, 9,10, 11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38