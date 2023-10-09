{{ config(materialized='table',
          schema= var('OVERBASE:SCHEMA')
) }}



SELECT 1 as one
FROM {{ source("firebase_crashlytics", "events") }}  

