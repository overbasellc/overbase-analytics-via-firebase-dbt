{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
     },
     incremental_strategy = 'insert_overwrite',
     require_partition_filter = true
) }}

{%- set custom_summed_metrics = [] -%}
{%- set builtinMeasures = [
  {"name":"user_engagement", "agg": "SUM(##)", "event_name": "user_engagement"},
  {"name":"ob_app_foreground", "agg": "SUM(##)", "event_name": "ob_app_foreground"}
] %}
{%- set allHealthMeasures = builtinMeasures + var("OVERBASE:CUSTOM_APP_HEALTH_MEASURES", []) %}
{%- for customHealthMeasure in allHealthMeasures -%}
    {# TODO check if name repescts an alias name. No spaces etc. #}
    {%- set user_column_name = customHealthMeasure['name'] %}
    {%- set additional_filter = customHealthMeasure["additional_filter"] if customHealthMeasure["additional_filter"] is defined else "True" -%}
    {%- set filter = "event_name = '" ~ customHealthMeasure["event_name"] ~ "' AND " ~ additional_filter -%}

    {%- for cnt in ["cnt", "users"] -%}
      {# do all for combinations for coc, cou (count over users), uou, uoc #}
        {%- set column_name = "" ~ user_column_name ~ "_" ~ cnt %}
        {%- set agg  = customHealthMeasure['agg'] | replace("##", "IF(" ~ filter    ~ ", " ~ cnt ~", 0)") -%}
        {%- set _ = custom_summed_metrics.append({"agg": agg ~ " as " ~ column_name, "alias": column_name }) -%}
    {%- endfor -%}
{%- endfor -%}

WITH analytics AS (
    SELECT  event_date
          , platform 
          , app_version.join_value as app_version_join_value
          , platform_version.join_value as platform_version_join_value
          , device_hardware.type AS device_hardware_type
          , device_hardware.manufacturer AS device_hardware_manufacturer
          , device_hardware.os_model AS device_hardware_os_model
          , {{ custom_summed_metrics |map(attribute='agg')|join("\n        ,") }}
    FROM {{ ref("fb_analytics_events") }}
    WHERE {{ overbase_firebase.analyticsDateFilterFor('event_date') }}
    AND event_name IN {{ tojson(allHealthMeasures | map(attribute="event_name") | list).replace("[", "(").replace("]", ")") }}
    GROUP BY 1,2,3,4,5,6,7
)
, crashlytics AS (
      SELECT  event_date
            , platform 
            , app_version.join_value as app_version_join_value
            , platform_version.join_value as platform_version_join_value
            , device_hardware.type AS device_hardware_type
            , device_hardware.manufacturer AS device_hardware_manufacturer
            , device_hardware.os_model AS device_hardware_os_model
            , SUM(cnt) as cnt
            , SUM(users) as users
  FROM {{ ref("fb_crashlytics_events") }}
  WHERE {{ overbase_firebase.crashlyticsDateFilterFor('event_date') }}
  GROUP BY 1,2,3,4,5,6,7

)
, joined_unpacked AS (
    SELECT  COALESCE(analytics.event_date ,  crashlytics.event_date ) AS event_date
          , COALESCE(analytics.platform ,  crashlytics.platform ) AS platform
          , COALESCE(analytics.app_version_join_value ,  crashlytics.app_version_join_value ) AS app_version_join_value
          , COALESCE(analytics.platform_version_join_value ,  crashlytics.platform_version_join_value ) AS platform_version_join_value
          , COALESCE(analytics.device_hardware_type ,  crashlytics.device_hardware_type ) AS device_hardware_type
          , COALESCE(analytics.device_hardware_manufacturer ,  crashlytics.device_hardware_manufacturer ) AS device_hardware_manufacturer
          , COALESCE(analytics.device_hardware_os_model ,  crashlytics.device_hardware_os_model ) AS device_hardware_os_model
          , {{ overbase_firebase.list_map_and_add_prefix(custom_summed_metrics | map(attribute='alias'), "analytics.") | join("\n          ,") }}
          , crashlytics.cnt as crashlytics_cnt
          , crashlytics.users as crashlytics_users
    FROM crashlytics
    FULL OUTER JOIN analytics ON 
            analytics.event_date = crashlytics.event_date
       AND  analytics.platform = crashlytics.platform
       AND  analytics.app_version_join_value = crashlytics.app_version_join_value
       AND  analytics.platform_version_join_value = crashlytics.platform_version_join_value
       AND  analytics.device_hardware_type = crashlytics.device_hardware_type
       AND  analytics.device_hardware_manufacturer = crashlytics.device_hardware_manufacturer
       AND  analytics.device_hardware_os_model = crashlytics.device_hardware_os_model
)
SELECT  event_date
      , platform 
      , {{ get_version_record_from_normalized('app_version_join_value') }} AS app_version
      , {{ get_version_record_from_normalized('platform_version_join_value') }} AS platform_version
      , STRUCT<type STRING, manufacturer STRING, os_model STRING>(
         device_hardware_type, device_hardware_manufacturer, device_hardware_os_model
      ) as device_hardware
     , crashlytics_cnt
     , crashlytics_users
     , {{ overbase_firebase.list_map_and_add_prefix(custom_summed_metrics | map(attribute='alias')) | join("\n          ,") }}
FROM joined_unpacked

-- For debugging
-- SELECT    SUM(IF(user_engagement_cnt IS NOT NULL AND crashlytics_cnt IS NOT NULL, 1, 0)) as both_not_null
--         , SUM(IF(user_engagement_cnt IS     NULL AND crashlytics_cnt IS NOT NULL, 1, 0)) as just_in_crashlytics
--         , SUM(IF(user_engagement_cnt IS NOT NULL AND crashlytics_cnt IS     NULL, 1, 0)) as just_in_analytics
--         , SUM(1) as total_cnt
-- FROM joined_unpacked
