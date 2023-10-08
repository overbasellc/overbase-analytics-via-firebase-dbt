
{{ config(materialized='ephemeral') }}


{{ compile_mandatory_var("OVERBASE:FIREBASE_PROJECT_ID", "overbase") }}
{{ compile_mandatory_var("OVERBASE:FIREBASE_ANALYTICS_DATASET_ID", "firebase_analytics_raw_test") }}
-- {{ compile_mandatory_var("OVERBASE:FIREBASE_ANALYTICS_EVENTS_TABLE_NAME", "events_*") }}
-- {{ compile_mandatory_var("OVERBASE:FIREBASE_ANALYTICS_EVENTS_INTRADAY_TABLE_NAME", "events_intraday_*") }}
 

 SELECT 1