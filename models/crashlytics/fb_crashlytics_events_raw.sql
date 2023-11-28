{{ overbase_firebase.verify_all_overbase_mandatory_variables() }}

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

-- https://firebase.google.com/docs/crashlytics/bigquery-export#without_stack_traces
SELECT    event_timestamp as event_ts
    , received_timestamp as received_ts
    , installation_uuid as crashlytics_user_pseudo_id
    , (SELECT value FROM UNNEST(custom_keys) WHERE key = 'fb_user_pseudo_id') as firebase_analytics_user_pseudo_id
    , COALESCE(user.id, (SELECT value FROM UNNEST(custom_keys) WHERE key = 'app_user_id')) as user_id
    , bundle_identifier as app_id
    , ARRAY_TO_STRING(ARRAY_REVERSE(SPLIT(bundle_identifier, '.')), '.') as reverse_app_id
    , event_id
         -- the platform we get in operating_system.type is not populated for Android, only for iOS. So rely on _TABLE_SUFFIX instead
    , CASE WHEN _TABLE_SUFFIX LIKE '%ANDROID%' THEN'ANDROID'
               WHEN _TABLE_SUFFIX LIKE '%IOS%' THEN'IOS'
               ELSE 'UNKNOWN' -- TODO: unit test for this
          END as platform 
    , STRUCT<id STRING, title STRING, subtitle STRING, variant_id STRING>(
        issue_id, issue_title, issue_subtitle, variant_id
    ) as issue
    , error_type
        , process_state
        , STRUCT<app STRING, device STRING>(
        	app_orientation, device_orientation
          ) as orientation
        , STRUCT<firebase_value STRING, build_no STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, major_minor_bugfix STRING, normalized INT64, join_value STRING>(
            {%- set v = "application.display_version" -%}
            {{ v }}, application.build_version, {{ overbase_firebase.get_version(v, "major") }}, {{ overbase_firebase.get_version(v, "minor") }}, {{ overbase_firebase.get_version(v, "bugfix") }}, {{ overbase_firebase.get_version(v, "major.minor") }}, {{ overbase_firebase.get_version(v, "major.minor.bugfix") }}, {{ overbase_firebase.get_version(v, "normalized") }}, COALESCE(CAST({{ overbase_firebase.get_version(v, "normalized") }} AS STRING), {{ v }} )
        ) AS app_version
        , STRUCT<firebase_value STRING, name STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, major_minor_bugfix STRING, normalized INT64, join_value STRING>(
            {%- set v = "operating_system.display_version" -%}
            {{ v }}, operating_system.name, {{ overbase_firebase.get_version(v, "major") }}, {{ overbase_firebase.get_version(v, "minor") }}, {{ overbase_firebase.get_version(v, "bugfix") }}, {{ overbase_firebase.get_version(v, "major.minor") }}, {{ overbase_firebase.get_version(v, "major.minor.bugfix") }}, {{ overbase_firebase.get_version(v, "normalized") }}, COALESCE(CAST( {{ overbase_firebase.get_version(v, "normalized") }} AS STRING), {{ v }} )
        ) AS platform_version
        , operating_system.modification_state as jailbroken_state
        , STRUCT<type STRING, manufacturer STRING, os_model STRING, architecture STRING>(
            LOWER(operating_system.device_type), LOWER(device.manufacturer), LOWER(device.model), device.architecture 
        ) AS device_hardware
        , {{ overbase_firebase.generate_struct_for_raw_crashlytics_custom_keys() }} as custom_keys
        , custom_keys as custom_keys_raw
        , STRUCT<used_bytes INT64, free_bytes INT64>(memory.used, memory.free) as memory
        , STRUCT<used_bytes INT64, free_bytes INT64>(storage.used, storage.free) as storage
        , STRUCT<name STRING, email STRING>(user.name, user.email) as user
        , crashlytics_sdk_version AS crashlytics_sdk_version_string
        , logs
        , breadcrumbs
        , blame_frame
        , exceptions as android_exceptions
        , errors as ios_non_fatal
        , threads
        , STRUCT<unity_version STRING, debug_build BOOLEAN, processor_type STRING, processor_count INTEGER, processor_frequency INTEGER, system_memory_size INTEGER, graphics_memory_size INTEGER, graphics_device_id INTEGER, graphics_device_vendor_id INTEGER, graphics_device_name STRING, graphics_device_vendor STRING, graphics_device_version STRING, graphics_device_type STRING, graphics_shader_level INTEGER, graphics_render_target_count INTEGER, graphics_copy_texture_support STRING, graphics_max_texture_size INTEGER, screen_size STRING, screen_dpi FLOAT64, screen_refresh_rate INTEGER, processor_frequency_mhz INTEGER, system_memory_size_mb INTEGER, graphics_memory_size_mb INTEGER, screen_size_px STRING, screen_refresh_rate_hz INTEGER, screen_resolution_dpi STRING>(
          {#  it has a short form of 20 columns (iOS REALTIME only) and a long form of 26 columns (Android historic, Android realtime & iOS historic )
          20:unity_version STRING,debug_build BOOLEAN,processor_type STRING,processor_count INTEGER,processor_frequency_mhz INTEGER,system_memory_size_mb INTEGER,graphics_memory_size_mb INTEGER,graphics_device_id INTEGER,graphics_device_vendor_id INTEGER,graphics_device_name STRING,graphics_device_vendor STRING,graphics_device_version STRING,graphics_device_type STRING,graphics_shader_level INTEGER,graphics_render_target_count INTEGER,graphics_copy_texture_support STRING,graphics_max_texture_size INTEGER,screen_size_px STRING,screen_refresh_rate_hz INTEGER,screen_resolution_dpi STRING,
          sometimes it's processor_frequency_mhz 
          26: unity_version STRING, debug_build BOOLEAN, processor_type STRING, processor_count INTEGER, processor_frequency INTEGER, system_memory_size INTEGER, graphics_memory_size INTEGER, graphics_device_id INTEGER, graphics_device_vendor_id INTEGER, graphics_device_name STRING, graphics_device_vendor STRING, graphics_device_version STRING, graphics_device_type STRING, graphics_shader_level INTEGER, graphics_render_target_count INTEGER, graphics_copy_texture_support STRING, graphics_max_texture_size INTEGER, screen_size STRING, screen_dpi FLOAT, screen_refresh_rate INTEGER, processor_frequency_mhz INTEGER, system_memory_size_mb INTEGER, graphics_memory_size_mb INTEGER, screen_size_px STRING, screen_refresh_rate_hz INTEGER, screen_resolution_dpi STRING
          Differences:
             + processor_frequency (but both also have processor_frequency_mhz)
             + system_memory_size (but both also have system_memory_size_mb)
             + graphics_memory_size (but both also have graphics_memory_size_mb)
             + screen_size
             + screen_dpi
             + screen_refresh_rate
            Those values are NULLed for the time being
          #}
          {{ overbase_firebase.list_map_and_add_prefix([
            "unity_version","debug_build","processor_type","processor_count",none,none,none,"graphics_device_id","graphics_device_vendor_id","graphics_device_name","graphics_device_vendor","graphics_device_version","graphics_device_type","graphics_shader_level","graphics_render_target_count","graphics_copy_texture_support","graphics_max_texture_size",none,none,none,"processor_frequency_mhz","system_memory_size_mb","graphics_memory_size_mb","screen_size_px","screen_refresh_rate_hz","screen_resolution_dpi"
            ], "unity_metadata." )| join(", ") }}
          ) AS unity_metadata
        , COUNT(1) OVER (PARTITION BY installation_uuid, event_id, variant_id) as duplicates_cnt
FROM {{ source("firebase_crashlytics", "events") }}  as events
WHERE True 
AND {{ overbase_firebase.crashlyticsTSFilterFor("event_timestamp") }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY installation_uuid, event_id, variant_id ORDER BY received_ts) = 1

