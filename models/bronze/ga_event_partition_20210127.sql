
{{ config(
    materialized='table',
    partition_by={
      "field": "created_time",
      "data_type": "timestamp",
      "granularity": "day",
      "time_ingestion_partitioning": false
    }
        ) }}

------------------------------------Add to config and change materialization to incremental when billing is enabled-----------------------------------
-- pre_hook="
--     CREATE TABLE if not exists `burger-builder-39571.ga4_marketing.ga_partition_sessions_20210131`
--     (
--     event_date STRING,
--     event_timestamp INT64,
--     event_name STRING,
--     event_params ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64>>>,
--     event_previous_timestamp INT64,
--     event_value_in_usd FLOAT64,
--     event_bundle_sequence_id INT64,
--     event_server_timestamp_offset INT64,
--     user_id STRING,
--     user_pseudo_id STRING,
--     privacy_info STRUCT<analytics_storage INT64, ads_storage INT64, uses_transient_token STRING>,
--     user_properties ARRAY<STRUCT<key INT64, value STRUCT<string_value INT64, int_value INT64, float_value INT64, double_value INT64, set_timestamp_micros INT64>>>,
--     user_first_touch_timestamp INT64,
--     user_ltv STRUCT<revenue FLOAT64, currency STRING>,
--     device STRUCT<category STRING, mobile_brand_name STRING, mobile_model_name STRING, mobile_marketing_name STRING, mobile_os_hardware_model INT64, operating_system STRING, operating_system_version STRING, vendor_id INT64, advertising_id INT64, language STRING, is_limited_ad_tracking STRING, time_zone_offset_seconds INT64, web_info STRUCT<browser STRING, browser_version STRING>>,
--     geo STRUCT<continent STRING, sub_continent STRING, country STRING, region STRING, city STRING, metro STRING>,
--     app_info STRUCT<id STRING, version STRING, install_store STRING, firebase_app_id STRING, install_source STRING>,
--     traffic_source STRUCT<medium STRING, name STRING, source STRING>,
--     stream_id INT64,
--     platform STRING,
--     event_dimensions STRUCT<hostname STRING>,
--     ecommerce STRUCT<total_item_quantity INT64, purchase_revenue_in_usd FLOAT64, purchase_revenue FLOAT64, refund_value_in_usd FLOAT64, refund_value FLOAT64, shipping_value_in_usd FLOAT64, shipping_value FLOAT64, tax_value_in_usd FLOAT64, tax_value FLOAT64, unique_items INT64, transaction_id STRING>,
--     items ARRAY<STRUCT<item_id STRING, item_name STRING, item_brand STRING, item_variant STRING, item_category STRING, item_category2 STRING, item_category3 STRING, item_category4 STRING, item_category5 STRING, price_in_usd FLOAT64, price FLOAT64, quantity INT64, item_revenue_in_usd FLOAT64, item_revenue FLOAT64, item_refund_in_usd FLOAT64, item_refund FLOAT64, coupon STRING, affiliation STRING, location_id STRING, item_list_id STRING, item_list_name STRING, item_list_index STRING, promotion_id STRING, promotion_name STRING, creative_name STRING, creative_slot STRING>>,
--     event_key DATE
--     )
--     PARTITION BY event_key
--     OPTIONS(
--     partition_expiration_days=60.0
--     );" ,
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

select 
*,
current_timestamp() created_time,
CASE
when event_name='first_visit' then DATE('2022-01-30')
when event_name='session_start' then DATE('2022-01-31')
when event_name='purchase' then DATE('2022-02-01')
else DATE('1999-01-01')
end as event_key
from {{ source('ga4_marketing', 'events_20210127') }}
where event_name in (
'first_visit'
,'session_start'
,'purchase'
)
