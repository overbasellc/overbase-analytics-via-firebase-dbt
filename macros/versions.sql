{% macro get_version(version, type) -%}
    {{ return(adapter.dispatch('get_version', 'overbase_firebase')(version, type)) }}
{%- endmacro %}

{% macro bigquery__get_version(version, type) -%}
    {%- if type == 'major' -%}
        SAFE_CAST(SPLIT({{ version }}, '.')[SAFE_OFFSET(0)] AS INT64)
    {%- elif type == 'minor' -%}
        SAFE_CAST(SPLIT({{ version }}, '.')[SAFE_OFFSET(1)] AS INT64)
    {%- elif type == 'bugfix' -%}
        SAFE_CAST(SPLIT({{ version }}, '.')[SAFE_OFFSET(2)] AS INT64)
    {%- elif type == 'major.minor' -%}
        SAFE_CAST(CONCAT( {{ overbase_firebase.get_version(version, 'major') }} , ".",  COALESCE({{ overbase_firebase.get_version(version, 'minor') }}, 0) ) AS FLOAT64)
    {%- elif type == 'major.minor.bugfix' -%}
        {#  17 -> 17; 17.0 -> 17.0; 17.0.1 -> 17.0.1  #}
        CASE WHEN REGEXP_CONTAINS({{ version }}, r'^[0-9.]*$') AND ARRAY_LENGTH(SPLIT({{ version }}, '.')) <= 3 THEN
                SAFE_CAST (
                    CONCAT(         SPLIT({{ version }}, '.')[SAFE_OFFSET(0)]
                         , COALESCE(CONCAT('.', SPLIT({{ version }}, '.')[SAFE_OFFSET(1)]),'')
                         , COALESCE(CONCAT('.', SPLIT({{ version }}, '.')[SAFE_OFFSET(2)]),'')
                    ) AS STRING)
            ELSE NULL
        END
    {%- elif type == 'normalized' -%}
        CASE WHEN REGEXP_CONTAINS({{ version }}, r'^[0-9.]*$') AND ARRAY_LENGTH(SPLIT({{ version }}, '.')) <= 3 THEN
                SAFE_CAST (
                    CONCAT(COALESCE(FORMAT('%06d', SAFE_CAST(SPLIT({{ version }}, '.')[SAFE_OFFSET(0)] AS INT64)),'000000')
                         , COALESCE(FORMAT('%06d', SAFE_CAST(SPLIT({{ version }}, '.')[SAFE_OFFSET(1)] AS INT64)),'000000')
                         , COALESCE(FORMAT('%06d', SAFE_CAST(SPLIT({{ version }}, '.')[SAFE_OFFSET(2)] AS INT64)),'000000')
                    ) AS INT64)
            ELSE NULL
        END
    {%- else -%}
       {{ exceptions.raise_compiler_error("Unknown type '%s'" % type) }}
    {%- endif -%}
{%- endmacro %}

