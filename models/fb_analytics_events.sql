{{ config(
    materialized='table',
    partition_by={
      "field": "created_date",
      "data_type": "date",
      "granularity": "day"
     }
) }}


{%- set columnNamesEventDimensions = ["created_dates", "app_id", "event_name", "platform", "appstore", "app_version", "platform_version",
                                "user_properties", "event_parameters",
                                "geo", "device_hardware", "device_language", "device_time_zone_offset",
                                "traffic_source"
] -%}

{%- set metricsToIgnore = get_event_parameter_tuples_metrics_only() -%}
{%- set miniColumnsToIgnoreInGroupBy = overbase_firebase.list_map_and_add_prefix(metricsToIgnore|map(attribute=5)|list, 'event_parameters.' ) -%}

{%- set tmp_res = overbase_firebase.get_filtered_columns_for_table("fb_analytics_events_raw", columnNamesEventDimensions, miniColumnsToIgnoreInGroupBy) -%}
{%- set columnsForEventDimensions = tmp_res[0] -%}
{%- set eventDimensionsUnnestedCount = tmp_res[1]  -%}

{%- set custom_summed_metrics = [] -%}
{%- for tuple in overbase_firebase.get_event_parameter_tuples_metrics_only () -%}
    {# cm = custom metris #}
    {%- set _ = custom_summed_metrics.append({"agg": "SUM(event_parameters." ~ tuple[5] ~ ") as cm_" ~ tuple[0], "alias": "cm_" ~ tuple[0]}) -%}
{%- endfor -%}

WITH data as (
    SELECT    DATE(created_at) as created_date
            , {{ overbase_firebase.unpack_columns_into_minicolumns(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, "", "") }}
            , COUNT(1) as cnt
            , COUNT(DISTINCT(user_pseudo_id)) as users
            {{ ", " if custom_summed_metrics|length > 0 else "" }} {{ custom_summed_metrics |map(attribute='agg')|join(", ") }}

    FROM {{ ref("fb_analytics_events_raw") }}
    GROUP BY 1 {% for n in range(2, 2 + eventDimensionsUnnestedCount) -%} ,{{ n }} {%- endfor %}
)
SELECT created_date
        , {{ overbase_firebase.pack_minicolumns_into_structs_for_select(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, "", "") }}
        , cnt
        , users
        , {{ custom_summed_metrics |map(attribute='alias')|join(", ") }}
FROM data
