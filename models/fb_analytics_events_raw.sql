{{ overbase_firebase.verify_all_overbase_mandatory_variables() }}
{{ config(
    materialized='table',
    partition_by={
      "field": "event_ts",
      "data_type": "timestamp",
      "granularity": "day"
     }
) }}

SELECT    TIMESTAMP_MICROS(event_timestamp) as event_ts
        , TIMESTAMP_MICROS(user_first_touch_timestamp) as install_ts
        , {{ overbase_firebase.calculate_age_between_timestamps("TIMESTAMP_MICROS(event_timestamp)", "TIMESTAMP_MICROS(user_first_touch_timestamp)") }} as install_age
        , user_pseudo_id
        , user_id
        , app_info.id as app_id
        , event_name
        , platform
        , app_info.install_source as appstore
        , STRUCT<firebase_value STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, normalized INT64>(
            app_info.version, {{ overbase_firebase.get_version("app_info.version", "major") }}, {{ overbase_firebase.get_version("app_info.version", "minor") }}, {{ overbase_firebase.get_version("app_info.version", "bugfix") }}, {{ overbase_firebase.get_version("app_info.version", "major.minor") }}, {{ overbase_firebase.get_version("app_info.version", "normalized") }}
        ) AS app_version
        , STRUCT<firebase_value STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, normalized INT64>(
            device.operating_system_version, {{ overbase_firebase.get_version("REPLACE(device.operating_system_version, device.operating_system ||' ','')", "major") }}, {{ overbase_firebase.get_version("REPLACE(device.operating_system_version, device.operating_system ||' ','')", "minor") }}, {{ overbase_firebase.get_version("REPLACE(device.operating_system_version, device.operating_system ||' ','')", "bugfix") }}, {{ overbase_firebase.get_version("REPLACE(device.operating_system_version, device.operating_system ||' ','')", "major.minor") }}, {{ overbase_firebase.get_version("REPLACE(device.operating_system_version, device.operating_system ||' ','')", "normalized") }}
        ) AS platform_version
        , {{ overbase_firebase.generate_struct_for_raw_user_properties() }} as user_properties
        , {{ overbase_firebase.generate_struct_for_raw_event_parameters() }} as event_parameters
        , STRUCT<city STRING , firebase_value STRING, iso_country_name STRING , iso_country_alpha_2 STRING, continent STRING, region STRING, sub_continent STRING, metro STRING>(
            geo.city, geo.country, country_codes.name, country_codes.alpha_2, geo.continent, geo.region , geo.sub_continent, geo.metro
        ) as geo
        , STRUCT<type STRING,brand_name STRING,model_name STRING,marketing_name STRING,os_hardware_model STRING>(
            device.category, device.mobile_brand_name, device.mobile_model_name, device.mobile_marketing_name, device.mobile_os_hardware_model 
        ) AS device_hardware
        , STRUCT<firebase_value STRING, iso_language_alpha_2 STRING, iso_country_alpha_2 STRING>(
            device.language, language_codes.alpha_2, language_region_codes.alpha_2
        ) AS device_language
        , IF(device.time_zone_offset_seconds >= 0,'+', '-') || LEFT(CAST(TIME(TIMESTAMP_SECONDS(ABS(device.time_zone_offset_seconds))) AS STRING),5) 
            AS device_time_zone_offset
        , STRUCT<name STRING, medium STRING, source STRING>(
            traffic_source.name, traffic_source.medium, traffic_source.source
        ) AS traffic_source
        , STRUCT<revenue FLOAT64, currency STRING>(
            user_ltv.revenue, user_ltv.currency
        ) AS users_ltv
        , STRUCT<firebase_app_id STRING, stream_id STRING, advertising_id STRING>(
            app_info.firebase_app_id, stream_id, device.advertising_id
        ) as other_ids
        , {{ overbase_firebase.generate_date_timezone_struct('TIMESTAMP_MICROS(event_timestamp)') }} as event_dates
        , {{ overbase_firebase.generate_date_timezone_struct('TIMESTAMP_MICROS(user_first_touch_timestamp)') }} as install_dates
        , COUNT(1) OVER (PARTITION BY user_pseudo_id, event_bundle_sequence_id, event_name, event_timestamp, event_previous_timestamp) as duplicates_cnt
FROM {{ source("firebase_analytics", "events") }}  as events
LEFT JOIN {{ref('iso_country')}} as country_codes
    ON LOWER(events.geo.country) = LOWER(country_codes.firebase_name)
LEFT JOIN {{ref("iso_language")}} as language_codes
    ON LOWER(SPLIT(events.device.language,'-')[SAFE_OFFSET(0)]) = language_codes.alpha_2
LEFT JOIN {{ref('iso_country')}} as language_region_codes -- some language have 3 parts (e.g. zh-hans-us), so just get the last one
    ON LOWER(ARRAY_REVERSE(SPLIT(events.device.language,'-'))[SAFE_OFFSET(0)]) = language_region_codes.alpha_2

WHERE True 
AND _TABLE_SUFFIX LIKE 'intraday%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, event_bundle_sequence_id, event_name, event_timestamp, event_previous_timestamp) = 1
