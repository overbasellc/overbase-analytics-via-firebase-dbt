{{ config(
    materialized='table',
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
     }
) }}

{%- set columnNamesToGroupBy = set(["app_id", "event_name", "platform", "appstore", "app_version"]) -%}
{%- set columnsUnnestedCount = 11 -%}

{%- set columns = adapter.get_columns_in_relation(ref("fb_analytics_events_raw")) -%}
{%- set columnsToGroupBy = [] -%}

{%- for column in columns -%}
  {%- if column.name in columnNamesToGroupBy -%}
    {%- set columnsUnnestedCount = columnsUnnestedCount + 1 -%}
    {%- if column.data_type.startswith('STRUCT') -%}
        {% set columnsUnnestedCount = columnsUnnestedCount + 1 + column.data_type.split(',')|length %}
    {%- else %}
        {% set columnsUnnestedCount = columnsUnnestedCount + 1 %}
    {%- endif -%}
        {{ columnsToGroupBy.append(column) or "" }}
  {%- endif -%}
{%- endfor %}

-- {% for column in columnsToGroupBy %}
--   Column: {{ column.name }} {{ column.data_type }}
-- {% endfor %}
-- columnsUnnestedCount {{ columnsUnnestedCount }}
-- get all columns
-- filter the ones we want for auto group by 
-- list the names of the ones we want in the CTE
-- add the group by count
-- select 

WITH data as (
    SELECT    DATE(created_at) as created_date
            , DATE(installed_at) as installed_date
    {%- for column in columnsToGroupBy -%}
        {%- if column.data_type.startswith('STRUCT') -%}
            {%- for structMiniColumn in column.data_type[7:-1].split(' ')[::2] %}
            , {{ column.name }}.{{ structMiniColumn }} as {{ column.name }}_{{ structMiniColumn }}
            {%- endfor -%}
        {%- else -%}
            , {{ column.name }} 
        {%- endif -%}
    {%- endfor -%}
            -- , platform_version.*
            -- , geo.*
            -- , device_hardware.*
            -- , device.*
            -- , traffic_source.*
            -- , user_properties
            -- , event_parameters
            , COUNT(1) as cnt
            , COUNT(DISTINCT(user_pseudo_id)) as users
    FROM {{ ref("fb_analytics_events_raw") }}
    GROUP BY 1,2 {% for n in range(3, 2 + columnsUnnestedCount) -%} ,{{ n }} {%- endfor %}
)
SELECT created_date, installed_date
        {%- for column in columnsToGroupBy -%}
        {%- if column.data_type.startswith('STRUCT') -%}
        , {{ column.data_type }} (
            {% for structMiniColumn in column.data_type[7:-1].split(' ')[::2] -%}
              {{ column.name }}_{{ structMiniColumn }} {{ ", " if not loop.last else "" }}
            {%- endfor %}
        ) as {{ column.name }}
        {%- else -%}
        , {{ column.name }} 
        {% endif -%}
        {%- endfor -%}
--     , STRUCT<original_value STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, normalized INT64>
--     (original_value, major, minor, bugfix, major_minor, normalized)
FROM data


-- GROUP BY DATE(created_at), DATE(installed_at), app_id, event_name, platform, appstore
--         , original_value, major, minor, bugfix, major_minor, normalized
--         , platform_version.original_value, platform_version.major, platform_version.minor, platform_version.bugfix, platform_version.major_minor, platform_version.normalized
--         , geo.city , geo.original_country_value, geo.country , geo.country_iso_alpha_2, geo.continent, geo.region, geo.sub_continent, geo.metro
--         , device_hardware.type,device_hardware.brand_name,device_hardware.model_name,device_hardware.marketing_name,device_hardware.os_hardware_model
--         , device.original_language_value, device.language_iso_alpha_2, device.language_region_iso_alpha_2, device.time_zone_offset
--         , traffic_source.name, traffic_source.medium, traffic_source.source

