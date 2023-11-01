{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
     },
     incremental_strategy = 'insert_overwrite',
     require_partition_filter = true
) }}

{%- set custom_summed_metrics = [] -%}
{%- set builtinMeasures = [
  {"name":"user_engagement", "agg": "SUM(##)", "event_name": "user_engagement"},
  {"name":"ob_app_foreground", "agg": "SUM(##)", "event_name": "ob_app_foreground"}
] %}
{%- set allHealthMeasures = builtinMeasures + var("OVERBASE:CUSTOM_APP_HEALTH_MEASURES", []) %}
{%- for customHealthMeasure in allHealthMeasures -%}
    {# TODO check if name repescts an alias name. No spaces etc. #}
    {%- set user_column_name = customHealthMeasure['name'] %}
    {%- set additional_filter = customHealthMeasure["additional_filter"] if customHealthMeasure["additional_filter"] is defined else "True" -%}
    {%- set filter = "event_name = '" ~ customHealthMeasure["event_name"] ~ "' AND " ~ additional_filter -%}

    {%- for cnt in ["cnt", "users"] -%}
      {# do all for combinations for coc, cou (count over users), uou, uoc #}
        {%- set column_name = "" ~ user_column_name ~ "_" ~ cnt %}
        {%- set agg  = customHealthMeasure['agg'] | replace("##", "IF(" ~ filter    ~ ", " ~ cnt ~", 0)") -%}
        {%- set _ = custom_summed_metrics.append({"agg": agg ~ " as " ~ column_name, "alias": column_name }) -%}
    {%- endfor -%}
{%- endfor -%}

WITH analytics AS (
    SELECT  {# ignore device_hardware.model_name & marketing_name in Analytics as they don't have a Crashlytics correspondent #}
            {%- set miniColumnsToIgnoreInGroupBy = ["device_hardware.model_name", "device_hardware.marketing_name"] %}
            {%- set tmp_res = overbase_firebase.get_filtered_columns_for_table("fb_analytics_events", ["event_date", "platform", "app_version", "platform_version", "device_hardware"], miniColumnsToIgnoreInGroupBy)-%}
            {%- set columnsForEventDimensions = tmp_res[0] -%}
            {%- set eventDimensionsUnnestedCount = tmp_res[1]  -%}
            {{ overbase_firebase.unpack_columns_into_minicolumns(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, [], "", "") }}
          , {{ custom_summed_metrics |map(attribute='agg')|join("\n        ,") }}


    FROM {{ ref("fb_analytics_events") }}
    WHERE {{ overbase_firebase.analyticsDateFilterFor('event_date') }}
    AND event_name IN {{ tojson(allHealthMeasures | map(attribute="event_name") | list).replace("[", "(").replace("]", ")") }}
    GROUP BY {% for n in range(1, eventDimensionsUnnestedCount + 1) -%} {{ "," if not loop.first else "" }}{{ n }} {%- endfor %}
)
, crashlytics AS (
      SELECT  {# ignore app_version.build_no & platform.name in Crashlytics as they don't have an Analytics correspondent #}
            {%- set miniColumnsToIgnoreInGroupBy = ["app_version.build_no", "platform_version.name", "device_hardware.architecture"] %}
            {%- set tmp_res = overbase_firebase.get_filtered_columns_for_table("fb_crashlytics_events", ["event_date", "platform", "app_version", "platform_version", "device_hardware"], miniColumnsToIgnoreInGroupBy)-%}
            {%- set columnsForEventDimensions = tmp_res[0] -%}
            {%- set eventDimensionsUnnestedCount = tmp_res[1]  -%}
            {{ overbase_firebase.unpack_columns_into_minicolumns(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, [], "", "") }}            
          , SUM(cnt) as cnt
          , SUM(users) as users
  FROM {{ ref("fb_crashlytics_events") }}
  WHERE {{ overbase_firebase.crashlyticsDateFilterFor('event_date') }}
  GROUP BY {% for n in range(1, eventDimensionsUnnestedCount + 1) -%} {{ "," if not loop.first else "" }}{{ n }} {%- endfor %}
)
, joined_unpacked AS (
    SELECT 
          {%- set unpackedColumnNamesWeGroupedOver = overbase_firebase.unpack_columns_into_minicolumns_array(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, [], "", "") | map(attribute=1) | list %} 
          {%- set analyticsAliases = overbase_firebase.list_map_and_add_prefix(unpackedColumnNamesWeGroupedOver, "analytics.") -%}
          {%- set crashlyticsAliases = overbase_firebase.list_map_and_add_prefix(unpackedColumnNamesWeGroupedOver, "crashlytics.") -%}
          {%- for joinTuple in zip(unpackedColumnNamesWeGroupedOver, analyticsAliases,crashlyticsAliases) %}
              {{ " ," if not loop.first else "" }} COALESCE({{ joinTuple[1] }} ,  {{ joinTuple[2] }} ) AS {{ joinTuple[0] }}
          {%- endfor %}
          , {{ overbase_firebase.list_map_and_add_prefix(custom_summed_metrics | map(attribute='alias'), "analytics.") | join("\n          ,") }}
          , crashlytics.cnt as crashlytics_cnt
          , crashlytics.users as crashlytics_users
    FROM crashlytics
    FULL OUTER JOIN analytics ON 
    {%- set unpackedColumnNamesWeGroupedOver = overbase_firebase.unpack_columns_into_minicolumns_array(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, [], "", "") | map(attribute=1) | list %} 
    {%- set analyticsAliases = overbase_firebase.list_map_and_add_prefix(unpackedColumnNamesWeGroupedOver, "analytics.") -%}
    {%- set crashlyticsAliases = overbase_firebase.list_map_and_add_prefix(unpackedColumnNamesWeGroupedOver, "crashlytics.") -%}
    {%- for joinTuple in zip(analyticsAliases,crashlyticsAliases) %}
      {{ " AND " if not loop.first else "" }} {{ joinTuple[0] ~ " = " ~ joinTuple[1] }}
    {%- endfor %}
)
SELECT  {{ overbase_firebase.pack_minicolumns_into_structs_for_select(columnsForEventDimensions, miniColumnsToIgnoreInGroupBy, "", "") }}
          , crashlytics_cnt
          , crashlytics_users
          , {{ overbase_firebase.list_map_and_add_prefix(custom_summed_metrics | map(attribute='alias')) | join("\n          ,") }}
FROM joined_unpacked