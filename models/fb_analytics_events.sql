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

{%- set miniColumnsToIgnoreInGroupBy = overbase_firebase.get_mini_columns_to_ignore_when_rolling_up() -%}

{%- set tmp_res = overbase_firebase.get_filtered_columns_for_table("fb_analytics_events_raw", columnNamesEventDimensions, miniColumnsToIgnoreInGroupBy) -%}
{%- set columnsForEventDimensions = tmp_res[0] -%}
{%- set eventDimensionsUnnestedCount = tmp_res[1]  -%}

{%- set custom_summed_metrics = [] -%}
{%- for tuple in overbase_firebase.get_event_parameter_tuples_for_rollup_metrics () -%}
    {# cm = custom metris #}
    {%- set _ = custom_summed_metrics.append({"agg": tuple[4]|replace("##", "event_parameters." ~ tuple[5]) ~ " as cm_" ~ tuple[5], "alias": "cm_" ~ tuple[5]}) -%}
{%- endfor -%}

WITH data as (
    SELECT   DATE(event_ts) as event_date
            , IF(install_age = 0, 'Age 0', 'Age > 0') as install_age_group
            , {{ overbase_firebase.unpack_columns_into_minicolumns(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, [], "", "") }}
            , COUNT(1) as cnt
            , COUNT(DISTINCT(user_pseudo_id)) as users
            {{ ", " if custom_summed_metrics|length > 0 else "" }} {{ custom_summed_metrics |map(attribute='agg')|join(", ") }}

    FROM {{ ref("fb_analytics_events_raw") }}
    WHERE {{ overbase_firebase.analyticsTSFilterFor('event_ts') }}
    GROUP BY 1,2 {% for n in range(3, 3 + eventDimensionsUnnestedCount) -%} ,{{ n }} {%- endfor %}
)
{%- set miniColumnsToAlsoNil = overbase_firebase.get_mini_columns_to_also_force_null_when_rolling_up() -%}
, nillableData as (
      SELECT   DATE(event_ts) as event_date
            , IF(install_age = 0, 'Age 0', 'Age > 0') as install_age_group
            , {{ overbase_firebase.unpack_columns_into_minicolumns(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, miniColumnsToAlsoNil, "", "") }}
            , COUNT(1) as cnt
            , COUNT(DISTINCT(user_pseudo_id)) as users
            {{ ", " if custom_summed_metrics|length > 0 else "" }} {{ custom_summed_metrics |map(attribute='agg')|join(", ") }}

    FROM {{ ref("fb_analytics_events_raw") }}
    WHERE {{ overbase_firebase.analyticsTSFilterFor('event_ts') }}
    GROUP BY 1,2 {% for n in range(3, 3 + eventDimensionsUnnestedCount) -%} ,{{ n }} {%- endfor %}
)
SELECT event_date
        , install_age_group
        , {{ overbase_firebase.pack_minicolumns_into_structs_for_select(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, "", "") }}
        , cnt
        , users
        {{ ", " if custom_summed_metrics|length > 0 else "" }}  {{ custom_summed_metrics |map(attribute='alias')|join(", ") }}
FROM data

UNION ALL 

SELECT event_date
        , install_age_group
        , {{ overbase_firebase.pack_minicolumns_into_structs_for_select(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, "", "") }}
        , cnt
        , users
        {{ ", " if custom_summed_metrics|length > 0 else "" }}  {{ custom_summed_metrics |map(attribute='alias')|join(", ") }}
FROM nillableData