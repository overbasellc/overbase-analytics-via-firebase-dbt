{{ config(
    materialized='table',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
     }
) }}
-- TODO: adi
    -- incremental_strategy='insert_overwrite',
    -- require_partition_filter = false

{%- if is_incremental() -%}
    {%- set dateCondition = "event_ts >= DATE_SUB(CURRENT_DATE(), INTERVAL " ~ var("OVERBASE:FIREBASE_DEFAULT_INCREMENTAL_DAYS", "5") ~ " DAY)" -%}
{%- else -%}
    {%- set dateCondition = "event_ts >= '" ~ var("OVERBASE:FIREBASE_ANALYTICS_FULL_REFRESH_START_DATE", "2018-01-01") ~ "'" -%}
{%- endif %}

 
SELECT DATE(event_ts) as event_date,
        * 
FROM {{ ref('fb_analytics_installs_raw') }} WHERE {{ dateCondition }}


