{{ config(
    materialized='table',
    partition_by={
      "field": "creation_date",
      "data_type": "date",
      "granularity": "day",
      "time_ingestion_partitioning": false
    }
) }}

WITH
  purchase AS(
  SELECT
    pseudo_id,
    1 AS purchase_flag
  FROM
    {{ref("ga_purchase")}} ),
  conversions AS(
  SELECT
    -- pseudo_id,
    DATE(created_time) creation_date,
    date,
    traffic_medium,
    traffic_source,
    category,
    mobile_brand_name,
    operating_system,
    platform,
    country,
    region,
    CASE
      WHEN p.purchase_flag=1 THEN 1
    ELSE
    0
  END
    conversion_flag,
  FROM
    {{ref("ga_session_start")}} ss
  LEFT JOIN
    purchase p
  ON
    ss.pseudo_id = p.pseudo_id )
    
SELECT
  creation_date,
  date,
  traffic_medium,
  traffic_source,
  category,
  mobile_brand_name,
  operating_system,
  platform,
  country,
  region,
  conversion_flag,
  COUNT(*) conversions
FROM
  conversions
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11