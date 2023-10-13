{# Returns the Database Columns & the count of the unnested ones.
    A column returned by adapter.get_columns_in_relation has multiple properties, for example:
        column.name: app_id     , column.data_type STRING
        column.name: app_version, column.data_type STRUCT<firebase_value STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, normalized INT64>

    This macro will return those 2 columns & the overall count, including the unnested mini columns
        2 columns: app_id STRING, app_version STRUCT<firebase_value STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, normalized INT64>
        contOfUnnested: 6
#}
{% macro get_filtered_columns_for_table(table_name, columnsToFind, miniColumnsToIgnore) -%}

    {%- set columnNamesToGroupBy = set(columnsToFind) -%}
    {%- set columnsUnnestedCount = [] -%}

    {%- set columns = adapter.get_columns_in_relation(ref(table_name)) -%}
    {%- set columnsToGroupBy = [] -%}

    {%- for column in columns -%}
        {%- if column.name in columnNamesToGroupBy -%}
            {%- if not column.data_type.startswith('STRUCT') -%}
                {% set _ = columnsUnnestedCount.append(1) %}
            {%- else %}
                {% set _ = columnsUnnestedCount.append(column.data_type.split(',')|length) %}
            {%- endif -%}
                {{ columnsToGroupBy.append(column) or "" }}
        {%- endif -%}
    {%- endfor %} 

    {{ return([columnsToGroupBy, columnsUnnestedCount | sum() - miniColumnsToIgnore|length ]) }}

{%- endmacro -%}

{# Returns just the column name if it's not a struct, otherwise it unnests it. Example: 
     app_id, event_name, platform, appstore
    , app_version.firebase_value as app_version_firebase_value
    , app_version.major as app_version_major
    , app_version.minor as app_version_minor
    , app_version.bugfix as app_version_bugfix
    , app_version.major_minor as app_version_major_minor
    , app_version.normalized as app_version_normalized
#}
{%- macro unpack_columns_into_minicolumns_for_select(columns, miniColumnsToIgnore) -%}
    {%- set miniColumnsToIgnoreSet = set(miniColumnsToIgnore) -%}

    {%- for column in columns -%}
        
        {%- if not column.data_type.startswith('STRUCT') -%}
            {{ ", " if not loop.first else "" }}{{ column.name }}
        {%- else -%}
            {# remove the 'STRUCT<' prefix, then split by ' ' and get every other item, ie the mini column name  #}
            {%- for structMiniColumn in column.data_type[7:-1].split(' ')[::2] -%}
                {%- if column.name ~ "." ~ structMiniColumn not in miniColumnsToIgnoreSet %}
                , {{ column.name }}.{{ structMiniColumn }} as {{ column.name }}_{{ structMiniColumn }}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}


{# Packs unpacked columns
app_id 
        , event_name 
        , platform 
        , appstore 
        , STRUCT<firebase_value STRING, major INT64, minor INT64, bugfix INT64, major_minor FLOAT64, normalized INT64> (
            app_version_firebase_value , app_version_major , app_version_minor , app_version_bugfix , app_version_major_minor , app_version_normalized 
        ) as app_version
        #}
{%- macro pack_minicolumns_into_structs_for_select(columns, miniColumnsToIgnore) -%}
    {%- set miniColumnsToIgnoreSet = set(miniColumnsToIgnore) -%}
    {%- for column in columns -%}
        {%- if not column.data_type.startswith('STRUCT') %}
            {{ ", " if not loop.first else "" }}{{ column.name }} 
        {%- else -%}
            {#- ['ob_view_name STRING',] -#}
            {%- set structDefinitionDDLs = [] %}
            {%- for structMiniColumnDDL in column.data_type[7:-1].split(',') -%}
                {% if column.name ~ "." ~ structMiniColumnDDL.strip().split(' ')[0] not in miniColumnsToIgnoreSet -%}
                    {%- set _ = structDefinitionDDLs.append(structMiniColumnDDL) -%}
                {%- endif -%}
            {%- endfor %}
            {%- set structValues = [] %}
            {% for structMiniColumn in column.data_type[7:-1].split(' ')[::2] -%}
                {% if column.name ~ "." ~ structMiniColumn not in miniColumnsToIgnoreSet -%}
                     {%- set _ = structValues.append(column.name ~ "_" ~ structMiniColumn) -%}
                {%- endif -%}
            {%- endfor -%}
            {{ ", " if not loop.first else "" }} STRUCT<{{ structDefinitionDDLs | join(", ") }}>(
                {{ structValues | join(", ") }} 
            ) as {{ column.name }}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}
