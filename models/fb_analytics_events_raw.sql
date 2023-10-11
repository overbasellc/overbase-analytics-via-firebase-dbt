{{ overbase_firebase.verify_all_overbase_mandatory_variables() }}
{{ config(
    materialized='table',
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
     }
) }}

SELECT
        TIMESTAMP_MICROS(event_timestamp) as created_at
    , TIMESTAMP_MICROS(user_first_touch_timestamp) as installed_at
    , user_pseudo_id
    , user_id
    , app_info.id as app_id
    , event_name
    , platform
    , app_info.install_source
    , STRUCT<revenue FLOAT64, currency STRING>(
        user_ltv.revenue, user_ltv.currency
    ) AS users_ltv
    -- TODO timezone offset seconds into string (e.g. -08:00)
    -- SELECT UNIX_MILLIS(TIMESTAMP '2008-12-25 15:30:00-08:00') AS millis;
    , STRUCT<language STRING, language_iso_2 STRING, time_zone_offset STRING>(
        device.language, 'TODO', 'TODO device.time_zone_offset_seconds'
    ) AS device
    , STRUCT<type STRING,brand_name STRING,model_name STRING,marketing_name STRING,os_hardware_model STRING>(
        device.category,device.mobile_brand_name,device.mobile_model_name,device.mobile_marketing_name,device.mobile_os_hardware_model 
    ) AS device_hardware
    , STRUCT<original_value STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, normalized INT64>(
        app_info.version, {{ overbase_firebase.get_version("app_info.version", "major") }}, {{ overbase_firebase.get_version("app_info.version", "minor") }}, {{ overbase_firebase.get_version("app_info.version", "bugfix") }}, {{ overbase_firebase.get_version("app_info.version", "major.minor") }}, {{ overbase_firebase.get_version("app_info.version", "normalized") }}
    ) AS app_version
    , STRUCT<original_value STRING, major INT64, bugfix INT64, major_minor FLOAT64, normalized INT64>(
        device.operating_system_version, {{ overbase_firebase.get_version("REPLACE(device.operating_system_version, device.operating_system ||' ','')", "major") }}, {{ overbase_firebase.get_version("REPLACE(device.operating_system_version, device.operating_system ||' ','')", "bugfix") }}, {{ overbase_firebase.get_version("REPLACE(device.operating_system_version, device.operating_system ||' ','')", "major.minor") }}, {{ overbase_firebase.get_version("REPLACE(device.operating_system_version, device.operating_system ||' ','')", "normalized") }}
    ) AS platform_version
    , STRUCT< name STRING, medium STRING, source STRING>(
        traffic_source.name, traffic_source.medium, traffic_source.source
    ) AS traffic_source
    , {{ overbase_firebase.generate_struct_for_user_properties() }} as user_properties
    , {{ overbase_firebase.generate_struct_for_event_parameters() }} as event_parameters
    , STRUCT<firebase_app_id STRING, stream_id STRING, advertising_id STRING>(
        app_info.firebase_app_id, stream_id, device.advertising_id
    ) as other_ids
    , COUNT(1) OVER (PARTITION BY user_pseudo_id, event_bundle_sequence_id, event_name, event_timestamp, event_previous_timestamp) as duplicates_cnt
FROM {{ source("firebase_analytics", "events") }}  
WHERE True 
AND _TABLE_SUFFIX LIKE 'intraday%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, event_bundle_sequence_id, event_name, event_timestamp, event_previous_timestamp) = 1
