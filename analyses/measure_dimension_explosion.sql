

{%- set columnNamesEventDimensions = ["created_dates", "installed_dates", "app_id", "event_name", "platform", "appstore", "app_version", "platform_version",
                                "user_properties", "event_parameters",
                                "geo", "device_hardware", "device_language", "device_time_zone_offset",
                                "traffic_source"
] -%}
{%- set miniColumnsToIgnoreInGroupBy = ["event_parameters.quantity_int"] -%}
{%- set tmp_res = overbase_firebase.get_filtered_columns_for_table("fb_analytics_events_raw", columnNamesEventDimensions, miniColumnsToIgnoreInGroupBy) -%}
{%- set columnsForEventDimensions = tmp_res[0] -%}


{%- set minicolumns = unpack_columns_into_minicolumns_array(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, "", "") -%}
{%- set unionAllSelects = [] -%}

WITH
{%- for column in minicolumns -%}
    {{ ", " if not loop.first else "" }} dim_{{loop.index}} AS ( SELECT COUNT(DISTINCT({{ column[0] }})) AS dist_cnt FROM  {{ ref("fb_analytics_events_raw") }})
    {% set _ = unionAllSelects.append("SELECT '" ~ column[1]  ~ "' AS dim_name, dist_cnt FROM dim_" ~ loop.index) -%}
{% endfor -%}
 
{{ unionAllSelects | join('\n UNION ALL ') }}

