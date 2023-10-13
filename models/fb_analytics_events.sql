{{ config(
    materialized='table',
    partition_by={
      "field": "created_date",
      "data_type": "date",
      "granularity": "day"
     }
) }}


{%- set columnNamesToGroupBy = ["app_id", "event_name", "platform", "appstore", "app_version", "platform_version",
                                "user_properties", "event_parameters",
                                "geo", "device_hardware", "device_language", "device_time_zone_offset",
                                "traffic_source"
] -%}
{%- set miniColumnsToIgnoreInGroupBy = ["event_parameters.quantity_int"] -%}
{%- set tmp_res = overbase_firebase.get_filtered_columns_for_table("fb_analytics_events_raw", columnNamesToGroupBy, miniColumnsToIgnoreInGroupBy) -%}
{%- set columnsToGroupBy = tmp_res[0] -%}
{%- set columnsUnnestedCount = tmp_res[1]  -%}

{%- set custom_summed_metrics = [] -%}
{%- for tuple in get_event_parameter_tuples () -%}
    {%- if tuple[2] == True -%}
        {# cm = custom metris #}
        {%- set _ = custom_summed_metrics.append({"agg": "SUM(event_parameters." ~ tuple[5] ~ ") as sum_" ~ tuple[0], "alias": "cm_" ~ tuple[0]}) -%}
    {% endif -%}
{%- endfor -%}

WITH data as (
    SELECT    DATE(created_at) as created_date
            , DATE(installed_at) as installed_date
            , {{ overbase_firebase.unpack_columns_into_minicolumns_for_select(columnsToGroupBy, miniColumnsToIgnoreInGroupBy) }}
            , COUNT(1) as cnt
            , COUNT(DISTINCT(user_pseudo_id)) as users
            , {{ custom_summed_metrics |map(attribute='agg')|join(", ") }}

    FROM {{ ref("fb_analytics_events_raw") }}
    GROUP BY 1,2 {% for n in range(3, 3 + columnsUnnestedCount) -%} ,{{ n }} {%- endfor %}
)
SELECT created_date, installed_date
        , {{ overbase_firebase.pack_minicolumns_into_structs_for_select(columnsToGroupBy, miniColumnsToIgnoreInGroupBy) }}
        , cnt
        , users
        , {{ custom_summed_metrics |map(attribute='alias')|join(", ") }}
FROM data
