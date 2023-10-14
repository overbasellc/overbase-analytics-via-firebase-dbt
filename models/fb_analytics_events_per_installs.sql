{{ config(
    materialized='table',
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
     }
) }}

SELECT *
FROM {{ ref("fb_analytics_events") }}
FROM {{ ref("fb_analytics_installs") }}