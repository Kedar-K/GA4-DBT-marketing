{{ config(
    materialized='table',
    partition_by={
      "field": "creation_date",
      "data_type": "date",
      "granularity": "day",
      "time_ingestion_partitioning": false
    }
) }}

select
    date(created_time) creation_date,
    date,
    traffic_medium,
    traffic_source,
    category,
    mobile_brand_name,
    operating_system,
    platform,
    country,
    region,
    count(*) acquisitions
from {{ ref("ga_session_start") }}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
