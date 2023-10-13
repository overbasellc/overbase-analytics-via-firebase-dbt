
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
        {% set outer_loop = loop %}

        {%- if not column.data_type.startswith('STRUCT') -%}
            {{ ", " if not outer_loop.first else "" }}{{ column.name }}
        {%- else -%}
            {# remove the 'STRUCT<' prefix, then split by ' ' and get every other item, ie the mini column name  #}
            {%- for structMiniColumn in column.data_type[7:-1].split(' ')[::2] -%}
                {%- if column.name ~ "." ~ structMiniColumn not in miniColumnsToIgnoreSet %}
                {{ "" if outer_loop.first and loop.first else ", " }} {{ column.name }}.{{ structMiniColumn }} as {{ column.name }}_{{ structMiniColumn }}
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
