{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
     },
    incremental_strategy = 'insert_overwrite',
    require_partition_filter = true,
) }}



SELECT
    event_date
    ,platform
    ,user_id
    ,user_pseudo_id
    ,count(*) as events_count
FROM  {{ ref("fb_analytics_events_raw") }}
WHERE {{ overbase_firebase.analyticsDateFilterFor('event_date') }}
GROUP by 1,2,3,4

