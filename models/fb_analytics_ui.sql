{{ config(
    materialized='table',
    partition_by={
      "field": "created_date",
      "data_type": "date",
      "granularity": "day"
     }
) }}

SELECT *
FROM {{ ref("fb_analytics_events") }}
WHERE event_name IN ('ob_ui_view_shown', 'ob_ui_button_tapped', 'ui_view_shown', 'ui_button_tapped')