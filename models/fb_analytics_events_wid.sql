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
{%- set columnNamesInstallDimensions = ["app_id", "event_name", "platform", "appstore", "app_version", "platform_version",
                                "user_properties", "event_parameters",
                                "geo", "device_hardware", "device_language", "device_time_zone_offset",
                                "traffic_source"
] -%}

{%- set miniColumnsToIgnoreInGroupBy = overbase_firebase.get_mini_columns_to_ignore_when_rolling_up() -%}

{%- set tmp_res = overbase_firebase.get_filtered_columns_for_table("fb_analytics_events_raw", columnNamesEventDimensions, miniColumnsToIgnoreInGroupBy) -%}
{%- set columnsForEventDimensions = tmp_res[0] -%}
{%- set eventDimensionsUnnestedCount = tmp_res[1]  -%}

{%- set tmp_res = overbase_firebase.get_filtered_columns_for_table("fb_analytics_installs_raw", columnNamesInstallDimensions, miniColumnsToIgnoreInGroupBy) -%}
{%- set columnsForInstallDimensions = tmp_res[0] -%}
{%- set installDimensionsUnnestedCount = tmp_res[1]  -%}
{# do these separaetely, so we don't end up with install_installed_dates #}
{%- set tmp_res = overbase_firebase.get_filtered_columns_for_table("fb_analytics_installs_raw", ["installed_dates"], miniColumnsToIgnoreInGroupBy) -%}
{%- set columnsForInstalledDatesDimension = tmp_res[0] -%}
{%- set installedDatesDimensionsUnnestedCount = tmp_res[1]  -%}


{%- set custom_summed_metrics = [] -%}
{%- for tuple in overbase_firebase.get_event_parameter_tuples_for_rollup_metrics() -%}
    {# cm = custom metrics #}
    {%- set _ = custom_summed_metrics.append({"agg": "SUM(events.event_parameters." ~ tuple[5] ~ ") as cm_" ~ tuple[0], "alias": "cm_" ~ tuple[0]}) -%}
{%- endfor -%}

WITH data as (
    SELECT    DATE(events.created_at) as created_date
            , {{ overbase_firebase.unpack_columns_into_minicolumns(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, "events.", "event_") }}

            , DATE(installs.installed_at) as installed_date
            , {{ overbase_firebase.unpack_columns_into_minicolumns(columnsForInstalledDatesDimension, miniColumnsToIgnoreInGroupBy, "installs.", "") }}
            , {{ overbase_firebase.unpack_columns_into_minicolumns(columnsForInstallDimensions, miniColumnsToIgnoreInGroupBy, "installs.", "install_") }}
            , COUNT(1) as cnt
            , COUNT(DISTINCT(events.user_pseudo_id)) as users
            {{ ", " if custom_summed_metrics|length > 0 else "" }} {{ custom_summed_metrics |map(attribute='agg')|join(", ") }}

    FROM {{ ref("fb_analytics_events_raw") }} as events
    LEFT JOIN {{ ref("fb_analytics_installs_raw") }} as installs ON events.user_pseudo_id = installs.user_pseudo_id
    -- TODO: max join on installs ?
    GROUP BY 1,2 {% for n in range(3, 3 + eventDimensionsUnnestedCount + installedDatesDimensionsUnnestedCount + installDimensionsUnnestedCount) -%} ,{{ n }} {%- endfor %}
)
SELECT created_date
        , {{ overbase_firebase.pack_minicolumns_into_structs_for_select(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, "event_", "") }}
        , installed_date
        , {{ overbase_firebase.pack_minicolumns_into_structs_for_select(columnsForInstalledDatesDimension, miniColumnsToIgnoreInGroupBy, "", "") }}
        , {{ overbase_firebase.pack_minicolumns_into_structs_for_select(columnsForInstallDimensions, miniColumnsToIgnoreInGroupBy, "install_", "install_") }}
        , cnt
        , users
        , {{ custom_summed_metrics |map(attribute='alias')|join(", ") }}
FROM data
