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

{%- set custom_summed_measures = [] -%}
{# mini_measures": ["cnt", "users"] are implicit in all of them, as the default #}
{%- set builtinMeasures = [
  {"model": "analytics", "name":"user_engagement"  , "agg": "SUM(##)", "event_name": "user_engagement"},
  {"model": "analytics", "name":"ob_app_foreground", "agg": "SUM(##)", "event_name": "ob_app_foreground"},
  {"model": "analytics", "name":"ob_app_background", "agg": "SUM(##)", "event_name": "ob_app_background"},
  {"model": "analytics", "name":"app_update"   , "agg": "SUM(##)", "event_name": "app_update"},
  {"model": "analytics", "name":"ob_app_update", "agg": "SUM(##)", "event_name": "ob_app_update"},

  {"model": "crashlytics", "name":"all_errors", "agg": "SUM(##)"},
  {"model": "crashlytics", "name":"fatal_crashes", "agg": "SUM(##)", "additional_filter": "error_type = 'FATAL'"},
  {"model": "crashlytics", "name":"fatal_foreground_crashes", "agg": "SUM(##)", "additional_filter": "error_type = 'FATAL' AND process_state = 'FOREGROUND'"},
  {"model": "crashlytics", "name":"fatal_background_crashes", "agg": "SUM(##)", "additional_filter": "error_type = 'FATAL' AND process_state = 'BACKGROUND' "}
] %}
{%- set allUnprocessedHealthMeasures = builtinMeasures + var("OVERBASE:CUSTOM_APP_HEALTH_MEASURES", []) %}
{%- set allAnalyticsEventNames = set([]) -%}
{%- for customHealthMeasure in allUnprocessedHealthMeasures -%}
    {%- set model = customHealthMeasure['model'] if customHealthMeasure['model'] is defined else "analytics" %}
    {%- if model not in ["analytics", "crashlytics"]-%}
        {{ exceptions.raise_compiler_error("Need to specify a valid model for each custom app health measure. Either 'analytics' or 'crashlytics'. Found " + model) }}
    {%- endif -%}
    {%- set user_column_name = customHealthMeasure['name'] %}
    {%- if " " in user_column_name -%}
        {{ exceptions.raise_compiler_error("Can't have spaces inside the name of a custom app health measure. It needs to be a valid column name." ) }}
    {%- endif -%}
    {%- set additional_filter = customHealthMeasure["additional_filter"] if customHealthMeasure["additional_filter"] is defined else "True" -%}
    {%- if customHealthMeasure["event_name"] is defined and model == 'analytics' -%}
      {%- set filter = "event_name = '" ~ customHealthMeasure["event_name"] ~ "' AND " ~ additional_filter -%}
      {%- set _ = allAnalyticsEventNames.add(customHealthMeasure["event_name"]) -%}
    {%- else -%}
      {%- set filter = additional_filter -%}
    {%- endif -%}
    {%- set mini_measures = customHealthMeasure["mini_measures"] if customHealthMeasure["mini_measures"] is defined else ["cnt", "users"] -%}
    {%- for cnt in mini_measures -%}
      {# do all for combinations for coc, cou (count over users), uou, uoc #}
        {%- set column_name = "" ~ user_column_name ~ "_" ~ cnt %}
        {%- set agg  = customHealthMeasure['agg'] | replace("##", "IF(" ~ filter    ~ ", " ~ cnt ~", 0)") -%}
        {%- set _ = custom_summed_measures.append({"model": model, "agg": agg ~ " as " ~ column_name, "alias": column_name }) -%}
    {%- endfor -%}
{%- endfor -%}

WITH analytics AS (
    SELECT  event_date
          , platform 
          , app_id
          , app_version.join_value as app_version_join_value
          , platform_version.join_value as platform_version_join_value
          , device_hardware.type AS device_hardware_type
          , device_hardware.manufacturer AS device_hardware_manufacturer
          , device_hardware.os_model AS device_hardware_os_model
          , {{ custom_summed_measures | selectattr("model", "equalto", "analytics") | map(attribute='agg')|join("\n          , ") }}
    FROM {{ ref("fb_analytics_events") }}
    WHERE {{ overbase_firebase.analyticsDateFilterFor('event_date') }}
    AND event_name IN {{ tojson(allAnalyticsEventNames| list).replace("[", "(").replace("]", ")") }}
    GROUP BY 1,2,3,4,5,6,7,8
)
, crashlytics AS (
      SELECT  event_date
            , platform 
            , app_id
            , app_version.join_value as app_version_join_value
            , platform_version.join_value as platform_version_join_value
            , device_hardware.type AS device_hardware_type
            , device_hardware.manufacturer AS device_hardware_manufacturer
            , device_hardware.os_model AS device_hardware_os_model
            , {{ custom_summed_measures | selectattr("model", "equalto", "crashlytics") | map(attribute='agg')|join("\n          , ") }}
  FROM {{ ref("fb_crashlytics_events") }}
  WHERE {{ overbase_firebase.crashlyticsDateFilterFor('event_date') }}
  GROUP BY 1,2,3,4,5,6,7,8

)
, joined_unpacked AS (
    SELECT  COALESCE(analytics.event_date ,  crashlytics.event_date ) AS event_date
          , COALESCE(analytics.platform ,  crashlytics.platform ) AS platform
          , COALESCE(analytics.app_id ,  crashlytics.app_id ) AS app_id
          , COALESCE(analytics.app_version_join_value ,  crashlytics.app_version_join_value ) AS app_version_join_value
          , COALESCE(analytics.platform_version_join_value ,  crashlytics.platform_version_join_value ) AS platform_version_join_value
          , COALESCE(analytics.device_hardware_type ,  crashlytics.device_hardware_type ) AS device_hardware_type
          , COALESCE(analytics.device_hardware_manufacturer ,  crashlytics.device_hardware_manufacturer ) AS device_hardware_manufacturer
          , COALESCE(analytics.device_hardware_os_model ,  crashlytics.device_hardware_os_model ) AS device_hardware_os_model
          , {{ overbase_firebase.list_map_and_add_prefix(custom_summed_measures | selectattr("model", "equalto", "analytics") | map(attribute='alias'), "analytics.") | join("\n          , ") }}
          , {{ overbase_firebase.list_map_and_add_prefix(custom_summed_measures | selectattr("model", "equalto", "crashlytics") | map(attribute='alias'), "crashlytics.") | join("\n          , ") }}
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
      , app_id
      , {{ overbase_firebase.get_version_record_from_normalized('app_version_join_value') }} AS app_version
      , {{ overbase_firebase.get_version_record_from_normalized('platform_version_join_value') }} AS platform_version
      , STRUCT<type STRING, manufacturer STRING, os_model STRING>(
         device_hardware_type, device_hardware_manufacturer, device_hardware_os_model
      ) as device_hardware
     , {{ overbase_firebase.list_map_and_add_prefix(custom_summed_measures | selectattr("model", "equalto", "analytics") | map(attribute='alias')) | join("\n          ,") }}
     , {{ overbase_firebase.list_map_and_add_prefix(custom_summed_measures | selectattr("model", "equalto", "crashlytics") | map(attribute='alias')) | join("\n          ,") }}
FROM joined_unpacked

-- For debugging
-- SELECT    SUM(IF(user_engagement_cnt IS NOT NULL AND crashlytics_cnt IS NOT NULL, 1, 0)) as both_not_null
--         , SUM(IF(user_engagement_cnt IS     NULL AND crashlytics_cnt IS NOT NULL, 1, 0)) as just_in_crashlytics
--         , SUM(IF(user_engagement_cnt IS NOT NULL AND crashlytics_cnt IS     NULL, 1, 0)) as just_in_analytics
--         , SUM(1) as total_cnt
-- FROM joined_unpacked
