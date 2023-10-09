{{ config(materialized='table'
) }}



SELECT 1 as one
FROM {{ source("firebase_crashlytics", "events") }}  

