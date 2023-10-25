{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
     },
    incremental_strategy = 'insert_overwrite',
    require_partition_filter = true
) }}
-- TODO: adi
    -- incremental_strategy='insert_overwrite',
    -- require_partition_filter = false


 
SELECT DATE(event_ts) as event_date,
        * 
FROM {{ ref('fb_analytics_installs_raw') }} 
WHERE {{ overbase_firebase.analyticsTSFilterFor('event_ts') }}


