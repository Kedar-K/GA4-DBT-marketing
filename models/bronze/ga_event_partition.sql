create or replace table `burger-builder-39571.ga4_marketing.ga_partition_sessions_20210131` partition by event_key as
select 
*,
CASE
when event_name='first_visit' then DATE('2022-01-30')
when event_name='session_start' then DATE('2022-01-31')
when event_name='purchase' then DATE('2022-02-01')
else DATE('1999-01-01')
end as event_key
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131` 

where event_name in (
'first_visit'
,'session_start'
,'purchase'
)
