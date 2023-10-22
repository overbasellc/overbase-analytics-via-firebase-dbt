{{ config(
    materialized='table',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
     }
) }}

SELECT 1 as dont_care
{# FROM {{ ref("fb_analytics_events") }} #}
{# FROM {{ ref("fb_analytics_installs") }} #}