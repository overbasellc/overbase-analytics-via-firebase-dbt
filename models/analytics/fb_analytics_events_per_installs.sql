{{ config(
    materialized='ephemeral'
) }}

SELECT 1 as dont_care
{# FROM {{ ref("fb_analytics_events") }} #}
{# FROM {{ ref("fb_analytics_installs") }} #}