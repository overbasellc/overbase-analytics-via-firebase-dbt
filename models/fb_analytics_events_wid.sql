{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
     },
    incremental_strategy = 'insert_overwrite'
) }}


{%- set columnNamesEventDimensions = ["app_id", "event_name", "platform", "appstore", "app_version", "platform_version",
                                "user_properties", "event_parameters",
                                "geo", "device_hardware", "device_language", "device_time_zone_offset",
                                "traffic_source", "event_dates"
] -%}
{%- set columnNamesInstallDimensions = ["app_id", "event_name", "platform", "appstore", "app_version", "platform_version",
                                "user_properties", "event_parameters",
                                "geo", "device_hardware", "device_language", "device_time_zone_offset",
                                "traffic_source", "install_dates"
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
    {%- set _ = custom_summed_metrics.append({"agg": tuple[4]|replace("##", "event_parameters." ~ tuple[5]) ~ " as cm_" ~ tuple[5], "alias": "cm_" ~ tuple[5]}) -%}
{%- endfor -%}

WITH data as (
    SELECT    DATE(events.event_ts) as event_date
            , {{ overbase_firebase.unpack_columns_into_minicolumns(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy,[],"events.", "event_") }}

            , DATE(installs.install_ts) as install_date
            , events.install_age as install_age
            , {{ overbase_firebase.unpack_columns_into_minicolumns(columnsForInstallDimensions, miniColumnsToIgnoreInGroupBy,[], "installs.", "install_") }}
            , COUNT(1) as cnt
            , COUNT(DISTINCT(events.user_pseudo_id)) as users
            {{ ", " if custom_summed_metrics|length > 0 else "" }} {{ custom_summed_metrics |map(attribute='agg')|join(", ") }}

    FROM {{ ref("fb_analytics_events_raw") }} as events
    LEFT JOIN {{ ref("fb_analytics_installs_raw") }} as installs ON events.user_pseudo_id = installs.user_pseudo_id
    WHERE {{ overbase_firebase.analyticsTSFilterFor('events.event_ts') }}
    -- TODO: max join on installs ?
    GROUP BY 1,2,3 {% for n in range(4, 4 + eventDimensionsUnnestedCount + installedDatesDimensionsUnnestedCount + installDimensionsUnnestedCount) -%} ,{{ n }} {%- endfor %}
)
{%- set miniColumnsToAlsoNil = overbase_firebase.get_mini_columns_to_also_force_null_when_rolling_up() -%}
, nillableData as (
    SELECT    DATE(events.event_ts) as event_date
            , {{ overbase_firebase.unpack_columns_into_minicolumns(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, miniColumnsToAlsoNil ,"events.", "event_") }}

            , DATE(installs.install_ts) as install_date
            , events.install_age as install_age
            , {{ overbase_firebase.unpack_columns_into_minicolumns(columnsForInstallDimensions, miniColumnsToIgnoreInGroupBy,miniColumnsToAlsoNil, "installs.", "install_") }}
            , COUNT(1) as cnt
            , COUNT(DISTINCT(events.user_pseudo_id)) as users
            {{ ", " if custom_summed_metrics|length > 0 else "" }} {{ custom_summed_metrics |map(attribute='agg')|join(", ") }}

    FROM {{ ref("fb_analytics_events_raw") }} as events
    LEFT JOIN {{ ref("fb_analytics_installs_raw") }} as installs ON events.user_pseudo_id = installs.user_pseudo_id
    WHERE {{ overbase_firebase.analyticsTSFilterFor('events.event_ts') }}
    -- TODO: max join on installs ?
    GROUP BY 1,2,3 {% for n in range(4, 4 + eventDimensionsUnnestedCount + installedDatesDimensionsUnnestedCount + installDimensionsUnnestedCount) -%} ,{{ n }} {%- endfor %}
)

SELECT event_date
        , {{ overbase_firebase.pack_minicolumns_into_structs_for_select(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, "event_", "") }}
        , install_age
        , install_date
        , {{ overbase_firebase.pack_minicolumns_into_structs_for_select(columnsForInstallDimensions, miniColumnsToIgnoreInGroupBy, "install_", "install_") }}
        , cnt
        , users
        , {{ custom_summed_metrics |map(attribute='alias')|join(", ") }}
FROM data

UNION ALL 


SELECT event_date
        , {{ overbase_firebase.pack_minicolumns_into_structs_for_select(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, "event_", "") }}
        , install_age
        , install_date
        , {{ overbase_firebase.pack_minicolumns_into_structs_for_select(columnsForInstallDimensions, miniColumnsToIgnoreInGroupBy, "install_", "install_") }}
        , cnt
        , users
        , {{ custom_summed_metrics |map(attribute='alias')|join(", ") }}
FROM nillableData
