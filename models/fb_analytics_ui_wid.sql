{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
     },
    incremental_strategy = 'insert_overwrite'
) }}

SELECT *
FROM {{ ref("fb_analytics_events_wid") }}
WHERE event_name IN ('ob_ui_view_shown', 'ob_ui_button_tapped', 'ui_view_shown', 'ui_button_tapped')
AND {{ overbase_firebase.analyticsDateFilterFor('event_date') }}