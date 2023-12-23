{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
     },
    incremental_strategy = 'insert_overwrite',
    require_partition_filter = true,
    cluster_by = ["platform", "app_id"],
) }}
    -- incremental_strategy='insert_overwrite',
    -- require_partition_filter = false

 
WITH  custom_install_event AS (
        SELECT * FROM {{ ref('fb_analytics_events_raw') }} WHERE {{ overbase_firebase.analyticsDateFilterFor('event_date') }}
        AND {% if var("OVERBASE:FIREBASE_ANALYTICS_CUSTOM_INSTALL_EVENT", "")|length > 0 -%}
                event_name = '{{ var("OVERBASE:FIREBASE_ANALYTICS_CUSTOM_INSTALL_EVENT", "") }}'
            {%- else -%}
                False
            {%- endif %}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_ts) = 1
)
, ob_install_event AS (
        SELECT * FROM {{ ref('fb_analytics_events_raw') }} WHERE event_name = 'ob_first_open' AND {{ overbase_firebase.analyticsDateFilterFor('event_date') }}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_ts) = 1
)
, fb_install_event AS (
        SELECT * FROM {{ ref('fb_analytics_events_raw') }} WHERE event_name = 'first_open' AND {{ overbase_firebase.analyticsDateFilterFor('event_date') }}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_ts) = 1
)
, user_pseudo_id_to_user_id AS (
        SELECT user_pseudo_id, user_id
        FROM {{ ref('fb_analytics_events_raw') }} WHERE user_id IS NOT NULL AND {{ overbase_firebase.analyticsDateFilterFor('event_date') }}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_ts) = 1
)

{%- set miniColumnsToIgnoreInGroupBy = ["duplicates_cnt"] -%}
{%- set columns = overbase_firebase.get_filtered_columns_for_table("fb_analytics_events_raw", "*", miniColumnsToIgnoreInGroupBy)[0] -%}

{%- set columnsInSelect = [] -%}
{%- for column in columns %}
    {%- if column.name == 'user_id' -%}
        {%- set _ = columnsInSelect.append("users.user_id") -%}
    {%- else -%}
        {%- set _ = columnsInSelect.append("COALESCE(custom." ~ column.name ~ ", ob." ~ column.name ~ ", fb." ~ column.name ~ ") as " ~ column.name) -%}
    {%- endif -%}
{%- endfor %}


, data as (
    SELECT   {{ columnsInSelect | join("\n           , ") }} 
    FROM fb_install_event as fb
    FULL OUTER JOIN ob_install_event as ob ON fb.user_pseudo_id = ob.user_pseudo_id
    FULL OUTER JOIN custom_install_event as custom ON fb.user_pseudo_id = custom.user_pseudo_id
    LEFT JOIN user_pseudo_id_to_user_id as users ON COALESCE(fb.user_pseudo_id, ob.user_pseudo_id, custom.user_pseudo_id) = users.user_pseudo_id
    WHERE True 
)
-- SELECT  COUNT(1) , COUNT(DISTINCT(user_pseudo_id))
SELECT *
FROM data 



