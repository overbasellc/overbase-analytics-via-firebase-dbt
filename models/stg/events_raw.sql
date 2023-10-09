{{ overbase_firebase.verify_all_overbase_mandatory_variables() }}

SELECT
    -- *
      DATE(timestamp_micros(event_timestamp)) as event_date
    , DATE_DIFF(DATE(timestamp_micros(event_timestamp)),DATE(timestamp_micros(user_first_touch_timestamp)),DAY) as age
    , user_pseudo_id
    , user_id
    , event_name
    , user_first_touch_timestamp as installed_date
    , STRUCT<revenue FLOAT64, currency STRING>(
        user_ltv.revenue, user_ltv.currency) as users_ltv
    , STRUCT<type STRING,brand_name STRING,model_name STRING,marketing_name STRING,os_hardware_model STRING>(
        device.category,device.mobile_brand_name,device.mobile_model_name,device.mobile_marketing_name,device.mobile_os_hardware_model ) as device
    , STRUCT<operating_system STRING, operating_system_version STRING>(
        device.operating_system, device.operating_system_version) as device_os
FROM {{ source("firebase_analytics", "events") }}  

