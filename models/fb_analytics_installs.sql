{{ config(
    materialized='table',
    partition_by={
      "field": "installed_date",
      "data_type": "date",
      "granularity": "day"
     }
) }}
-- TODO: adi
    -- incremental_strategy='insert_overwrite',
    -- require_partition_filter = false

{%- if is_incremental() -%}
    {%- set dateCondition = "created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL " ~ var("OVERBASE:FIREBASE_DEFAULT_INCREMENTAL_DAYS", "5") ~ " DAY)" -%}
{%- else -%}
    {%- set dateCondition = "created_at >= '" ~ var("OVERBASE:FIREBASE_ANALYTICS_FULL_REFRESH_START_DATE", "2018-01-01") ~ "'" -%}
{%- endif %}

 
SELECT * FROM {{ ref('fb_analytics_installs_raw') }} WHERE {{ dateCondition }}


