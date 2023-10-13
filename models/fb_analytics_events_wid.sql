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
{%- set miniColumnsToIgnore = ["event_parameters.quantity_int"] -%}
{%- set tmp_res = overbase_firebase.get_filtered_columns_for_table("fb_analytics_events_raw", columnNamesEventDimensions, miniColumnsToIgnore) -%}
{%- set columnsForEventDimensions = tmp_res[0] -%}
{%- set eventDimensionsUnnestedCount = tmp_res[1]  -%}

{%- set tmp_res = overbase_firebase.get_filtered_columns_for_table("fb_analytics_installs_raw", columnNamesInstallDimensions, miniColumnsToIgnore) -%}
{%- set columnsForInstallDimensions = tmp_res[0] -%}
{%- set installDimensionsUnnestedCount = tmp_res[1]  -%}
{# do these separaetely, so we don't end up with install_installed_dates #}
{%- set tmp_res = overbase_firebase.get_filtered_columns_for_table("fb_analytics_installs_raw", ["installed_dates"], miniColumnsToIgnore) -%}
{%- set columnsForInstalledDatesDimension = tmp_res[0] -%}
{%- set installedDatesDimensionsUnnestedCount = tmp_res[1]  -%}


{%- set custom_summed_metrics = [] -%}
{%- for tuple in overbase_firebase.get_event_parameter_tuples () -%}
    {%- if tuple[2] == True -%}
        {# cm = custom metrics #}
        {%- set _ = custom_summed_metrics.append({"agg": "SUM(events.event_parameters." ~ tuple[5] ~ ") as cm_" ~ tuple[0], "alias": "cm_" ~ tuple[0]}) -%}
    {% endif -%}
{%- endfor -%}

WITH data as (
    SELECT    DATE(events.created_at) as created_date
            , {{ overbase_firebase.unpack_columns_into_minicolumns_for_select(columnsForEventDimensions, miniColumnsToIgnore, "events.", "event_") }}

            , DATE(installs.installed_at) as installed_date
            , {{ overbase_firebase.unpack_columns_into_minicolumns_for_select(columnsForInstalledDatesDimension, miniColumnsToIgnore, "installs.", "") }}
            , {{ overbase_firebase.unpack_columns_into_minicolumns_for_select(columnsForInstallDimensions, miniColumnsToIgnore, "installs.", "install_") }}
            , COUNT(1) as cnt
            , COUNT(DISTINCT(events.user_pseudo_id)) as users
            , {{ custom_summed_metrics |map(attribute='agg')|join(", ") }}

    FROM {{ ref("fb_analytics_events_raw") }} as events
    LEFT JOIN {{ ref("fb_analytics_installs_raw") }} as installs ON events.user_pseudo_id = installs.user_pseudo_id
    -- TODO: max join on installs ?
    GROUP BY 1,2 {% for n in range(3, 3 + eventDimensionsUnnestedCount + installedDatesDimensionsUnnestedCount + installDimensionsUnnestedCount) -%} ,{{ n }} {%- endfor %}
)
SELECT created_date
        , {{ overbase_firebase.pack_minicolumns_into_structs_for_select(columnsForEventDimensions, miniColumnsToIgnore, "event_", "") }}
        , installed_date
        , {{ overbase_firebase.pack_minicolumns_into_structs_for_select(columnsForInstalledDatesDimension, miniColumnsToIgnore, "", "") }}
        , {{ overbase_firebase.pack_minicolumns_into_structs_for_select(columnsForInstallDimensions, miniColumnsToIgnore, "install_", "install_") }}
        , cnt
        , users
        , {{ custom_summed_metrics |map(attribute='alias')|join(", ") }}
FROM data
