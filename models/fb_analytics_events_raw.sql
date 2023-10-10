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
    , event_name
    , platform
    , app_info.id as bundle_id
    , app_info.firebase_app_id as firebase_app_id
    , stream_id 
    , device.advertising_id as advertising_id
    , device.language as device_language
    , device.time_zone_offset_seconds
    , app_info.install_source
    , STRUCT<revenue FLOAT64, currency STRING>(
        user_ltv.revenue, user_ltv.currency) as users_ltv
    , STRUCT<type STRING,brand_name STRING,model_name STRING,marketing_name STRING,os_hardware_model STRING>(
        device.category,device.mobile_brand_name,device.mobile_model_name,device.mobile_marketing_name,device.mobile_os_hardware_model ) as device
    , STRUCT<`full` STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, normalized INT64>(
        app_info.version, {{ overbase_firebase.get_version("app_info.version", "major") }}, {{ overbase_firebase.get_version("app_info.version", "minor") }}, {{ overbase_firebase.get_version("app_info.version", "bugfix") }}, {{ overbase_firebase.get_version("app_info.version", "major.minor") }}, {{ overbase_firebase.get_version("app_info.version", "normalized") }}
    ) as app_version
     , STRUCT<`full` STRING, major INT64, bugfix INT64, major_minor FLOAT64, normalized INT64>(
       replace(device.operating_system_version, device.operating_system ||' ',''), {{ overbase_firebase.get_version("replace(device.operating_system_version, device.operating_system ||' ','')", "major") }},
        {{ overbase_firebase.get_version("replace(device.operating_system_version, device.operating_system ||' ','')", "bugfix") }},
        {{ overbase_firebase.get_version("replace(device.operating_system_version, device.operating_system ||' ','')", "major.minor") }},
        {{ overbase_firebase.get_version("replace(device.operating_system_version, device.operating_system ||' ','')", "normalized") }}
         ) as device_operating_system_version
    , STRUCT< name STRING, medium STRING, source STRING>(
        traffic_source.name, traffic_source.medium, traffic_source.source
    ) as traffic_source
    , {{ overbase_firebase.get_user_properties() }} as user_properties
    , {{ overbase_firebase.get_event_parameters() }} as event_parameters

FROM {{ source("firebase_analytics", "events") }}  
WHERE True 
AND _TABLE_SUFFIX LIKE 'intraday%'
--TODO: group by/distinct 
