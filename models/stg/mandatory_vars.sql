
{{ config(materialized='ephemeral') }}


{{ compile_mandatory_var("OVERBASE:FIREBASE_PROJECT_ID", "overbase") }}
{{ compile_mandatory_var("OVERBASE:FIREBASE_ANALYTICS_DATASET_ID", "firebase_analytics_raw_test") }}
 

 SELECT 1
