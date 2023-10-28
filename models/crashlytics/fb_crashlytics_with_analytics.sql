{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_ts",
      "data_type": "timestamp",
      "granularity": "day"
     },
     incremental_strategy = 'insert_overwrite',
     require_partition_filter = true
) }}

{%- set customEventsForCrashRate = ['user_engagement', 'ob_app_foreground'] + var("OVERBASE:FIREBASE_CRASHLYTICS_ANALYTICS_EVENTS_FOR_CRASH_RATE", []) -%}

WITH analytics AS (
    SELECT    event_date
            , platform
            , COALESCE(CAST(app_version.normalized AS STRING), app_version.firebase_value) AS app_version_joinable
            , COALESCE(CAST(platform_version.normalized AS STRING), platform_version.firebase_value) AS platform_version_joinable
            , device_hardware.type AS device_hardware_type
            , device_hardware.manufacturer AS device_hardware_manufacturer
            , device_hardware.os_model AS device_hardware_os_model
        {%- for customEvent in customEventsForCrashRate %}
            , SUM(IF(event_name = {{ "'" ~ customEvent ~ "'" }},   cnt, 0)) AS {{ customEvent }}_cnt
            , SUM(IF(event_name = {{ "'" ~ customEvent ~ "'" }}, users, 0)) AS {{ customEvent }}_users
        {%- endfor %}

    FROM {{ ref("fb_analytics_events") }}
    WHERE {{ overbase_firebase.analyticsDateFilterFor('event_date') }}
    AND event_name IN {{ tojson(customEventsForCrashRate).replace("[", "(").replace("]", ")") }}
    GROUP BY 1,2,3,4,5,6,7
)

SELECT crashlytics.*
    {%- for customEvent in customEventsForCrashRate %}
        , analytics.{{ customEvent }}_cnt
        , analytics.{{ customEvent }}_users
    {%- endfor %}
FROM {{ ref("fb_crashlytics_events") }} AS crashlytics
LEFT JOIN analytics ON 
        crashlytics.event_date = analytics.event_date
    AND crashlytics.platform = analytics.platform
    AND COALESCE(CAST(crashlytics.app_version.normalized AS STRING), crashlytics.app_version.firebase_value)  = analytics.app_version_joinable
    AND COALESCE(CAST(crashlytics.platform_version.normalized AS STRING), crashlytics.platform_version.firebase_value)  = analytics.platform_version_joinable
    AND device_hardware.type = analytics.device_hardware_type
    AND device_hardware.manufacturer = analytics.device_hardware_manufacturer
    AND device_hardware.os_model = analytics.device_hardware_os_model
WHERE {{ overbase_firebase.crashlyticsDateFilterFor('crashlytics.event_date') }}
