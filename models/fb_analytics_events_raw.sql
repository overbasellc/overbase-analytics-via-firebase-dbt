{{ overbase_firebase.verify_all_overbase_mandatory_variables() }}
{{ config(
    materialized='table',
) }}

    -- materialized='incremental',
    -- incremental_strategy='insert_overwrite',
    -- require_partition_filter = false
    -- partition_by={
    --   "field": "created_at",
    --   "data_type": "date",
    --   "granularity": "day"
    -- },

-- SELECT 
    /* {{ overbase_firebase.get_version("app_info.version", "normalized") }} */
SELECT
    -- *
      TIMESTAMP_MICROS(event_timestamp) as created_at
    , TIMESTAMP_MICROS(user_first_touch_timestamp) as installed_at
    , user_pseudo_id
    , user_id
    , event_name
    , STRUCT<revenue FLOAT64, currency STRING>(
        user_ltv.revenue, user_ltv.currency) as users_ltv
    , STRUCT<type STRING,brand_name STRING,model_name STRING,marketing_name STRING,os_hardware_model STRING>(
        device.category,device.mobile_brand_name,device.mobile_model_name,device.mobile_marketing_name,device.mobile_os_hardware_model ) as device
    , STRUCT<operating_system STRING, operating_system_version STRING>(
        device.operating_system, device.operating_system_version) as device_os
    , STRUCT<`full` STRING,major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, normalized INT64>(
        app_info.version, {{ overbase_firebase.get_version("app_info.version", "major") }}, {{ overbase_firebase.get_version("app_info.version", "minor") }}, {{ overbase_firebase.get_version("app_info.version", "bugfix") }}, {{ overbase_firebase.get_version("app_info.version", "major.minor") }}, {{ overbase_firebase.get_version("app_info.version", "normalized") }}
    ) as app_version
    -- , STRUCT<name_string STRING, view_name_string STRING, view_type_string STRING> 
    --     as event_parameters
    -- , STRUCT<>
    -- as user_properties
FROM {{ source("firebase_analytics", "events") }}  
WHERE True 
AND _TABLE_SUFFIX LIKE 'intraday%'
