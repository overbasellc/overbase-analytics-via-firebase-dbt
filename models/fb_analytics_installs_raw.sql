{{ config(
    materialized='table',
    partition_by={
      "field": "installed_at",
      "data_type": "timestamp",
      "granularity": "day"
     }
) }}
    -- incremental_strategy='insert_overwrite',
    -- require_partition_filter = false

{%- if is_incremental() -%}
    {%- set dateCondition = "created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL " ~ var("OVERBASE:FIREBASE_DEFAULT_INCREMENTAL_DAYS") ~ " DAY)" -%}
{%- else -%}
    {%- set dateCondition = "created_at >= '" ~ var("OVERBASE:FIREBASE_ANALYTICS_FULL_REFRESH_START_DATE") ~ "'" -%}
{%- endif %}

 
WITH  custom_install_event AS (
        SELECT * FROM {{ ref('fb_analytics_events_raw') }} WHERE {{ dateCondition }}
        AND {% if var("OVERBASE:FIREBASE_ANALYTICS_CUSTOM_INSTALL_EVENT")|length > 0 -%}
                event_name = '{{ var("OVERBASE:FIREBASE_ANALYTICS_CUSTOM_INSTALL_EVENT") }}'
            {%- else -%}
                False
            {%- endif %}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY created_at) = 1
)
, ob_install_event AS (
        SELECT * FROM {{ ref('fb_analytics_events_raw') }} WHERE {{ dateCondition }} AND event_name = 'ob_first_open'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY created_at) = 1
)
, fb_install_event AS (
        SELECT * FROM {{ ref('fb_analytics_events_raw') }} WHERE {{ dateCondition }} AND event_name = 'first_open'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY created_at) = 1
)

{%- set miniColumnsToIgnoreInGroupBy = ["duplicates_cnt"] -%}
{%- set columns = overbase_firebase.get_filtered_columns_for_table("fb_analytics_events_raw", "*", miniColumnsToIgnoreInGroupBy)[0] -%}

, data as (
    SELECT   
        {%- for column in columns %}
            {{ ", " if not loop.first else "" }} COALESCE(custom.{{ column.name }}, ob.{{ column.name }}, fb.{{ column.name }}) as {{ column.name }}
        {%- endfor %}
    FROM fb_install_event as fb
    FULL OUTER JOIN ob_install_event as ob ON fb.user_pseudo_id = ob.user_pseudo_id
    FULL OUTER JOIN custom_install_event as custom ON fb.user_pseudo_id = custom.user_pseudo_id
    WHERE True 
)
-- SELECT  COUNT(1) , COUNT(DISTINCT(user_pseudo_id))
SELECT *
FROM data 


