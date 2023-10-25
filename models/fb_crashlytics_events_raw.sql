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
		, event_id
		, variant_id
		, CASE WHEN _TABLE_SUFFIX LIKE '%ANDROID%' THEN'ANDROID'
               WHEN _TABLE_SUFFIX LIKE '%IOS%' THEN'IOS'
               ELSE 'UNKNOWN'
          END as platform 
		, STRUCT<id STRING, title STRING, subtitle STRING>(
			issue_id, issue_title, issue_subtitle
 		 ) as issue
		, error_type
        , process_state
        , STRUCT<app STRING, device STRING>(
        	app_orientation, device_orientation
          ) as orientation
		-- the platform we get in operating_system.type is not populated for Android, only for iOS. So rely on _TABLE_SUFFIX instead
        , STRUCT<firebase_value STRING, build_no STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, major_minor_bugfix STRING, normalized INT64>(
            application.display_version, application.build_version, {{ overbase_firebase.get_version("application.display_version", "major") }}, {{ overbase_firebase.get_version("application.display_version", "minor") }}, {{ overbase_firebase.get_version("application.display_version", "bugfix") }}, {{ overbase_firebase.get_version("application.display_version", "major.minor") }}, {{ overbase_firebase.get_version("application.display_version", "major.minor.bugfix") }}, {{ overbase_firebase.get_version("application.display_version", "normalized") }}
        ) AS app_version
        , STRUCT<firebase_value STRING, name STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, major_minor_bugfix STRING, normalized INT64>(
            operating_system.display_version, operating_system.name, {{ overbase_firebase.get_version("operating_system.display_version", "major") }}, {{ overbase_firebase.get_version("operating_system.display_version", "minor") }}, {{ overbase_firebase.get_version("operating_system.display_version", "bugfix") }}, {{ overbase_firebase.get_version("operating_system.display_version", "major.minor") }}, {{ overbase_firebase.get_version("operating_system.display_version", "major.minor.bugfix") }}, {{ overbase_firebase.get_version("operating_system.display_version", "normalized") }}
        ) AS platform_version
        , operating_system.modification_state as jailbroken_state
		, memory -- record with used & free
		, storage -- record with used & free
        , STRUCT<type STRING, manufacturer STRING, os_hardware_model STRING, architecture STRING>(
            LOWER(operating_system.device_type), device.manufacturer, device.model, device.architecture 
        ) AS device_hardware
        , STRUCT<name STRING, email STRING>(
        	user.name, user.email
        ) as user
        , crashlytics_sdk_version AS crashlytics_sdk_version_string
        , {{ overbase_firebase.generate_struct_for_raw_crashlytics_custom_keys() }} as custom_keys
        , custom_keys as custom_keys_raw
        , logs
        , breadcrumbs
        , blame_frame
        , exceptions as android_exceptions
        , errors as ios_non_fatal
        , threads
        , unity_metadata
        , COUNT(1) OVER (PARTITION BY installation_uuid, event_id, variant_id) as duplicates_cnt
FROM {{ source("firebase_crashlytics", "events") }}  as events
WHERE True 
AND {{ overbase_firebase.crashlyticsTSFilterFor("event_timestamp") }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY installation_uuid, event_id, variant_id ORDER BY received_ts) = 1

