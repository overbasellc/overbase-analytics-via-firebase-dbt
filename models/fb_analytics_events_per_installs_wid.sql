{{ config(
    materialized='table',
    partition_by={
      "field": "created_date",
      "data_type": "date",
      "granularity": "day"
     }
) }}

SELECT 1
{# FROM {{ ref("fb_analytics_events_wid") }} #}
{# FROM {{ ref("fb_analytics_installs") }} #}