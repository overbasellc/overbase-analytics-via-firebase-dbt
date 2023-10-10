{% macro get_user_properties() -%}
    {% set builtin_parameters = [ 
            ["ob_ui_dark_mode", "STRING"]
           ,["ob_ui_font_size", "STRING"]
    ]%}

    {{ generate_struct((builtin_parameters + flatten_properties(var('OVERBASE:CUSTOM_USER_PROPERTIES', []))), 'user_properties') }}
{%- endmacro %}


{% macro get_event_parameters() -%}
    {% set builtin_parameters = [ 
            ["ob_view_name", "STRING"]
           ,["ob_view_type", "STRING"]
           ,["ob_parent_view_name", "STRING"]
           ,["ob_parent_view_type", "STRING"]
           ,["ob_name", "STRING"]
    ]%}

    {{ generate_struct((builtin_parameters +  flatten_properties(var('OVERBASE:CUSTOM_EVENT_PARAMETERS', []))), 'event_parameters') }}
{%- endmacro %}


{# Flatten to a simple array of arrays [['my_custom_user_prop', 'string']] #}
{% macro flatten_properties(custom_array_of_dicts) -%}
    {% set flat_result = [] %}
    {% for dict in custom_array_of_dicts %}
        {{ flat_result.append([dict['property_name'], dict['data_type']]) }}
    {% endfor %}
    {{ return(flat_result) }}
{% endmacro %}



{% macro generate_struct(all_parameters, firebase_record_name) -%}
    {{ return(adapter.dispatch('generate_struct', 'overbase_firebase')(all_parameters, firebase_record_name) ) }}
{%- endmacro %}


{# STRUCT<ob_ui_dark_mode_string STRING, plays_progressive_string STRING, first_open_time_int INT64, poorly_set_variable_double DOUBLE>(
     (SELECT LOWER(value.string_value) from UNNEST(user_properties) WHERE key = 'ob_ui_dark_mode') , (SELECT LOWER(value.string_value) from UNNEST(user_properties) WHERE key = 'plays_progressive') , (SELECT value.int_value from UNNEST(user_properties) WHERE key = 'first_open_time') , (SELECT value.double_value from UNNEST(user_properties) WHERE key = 'poorly_set_variable') 
 )  #}
{% macro bigquery__generate_struct(all_parameters, firebase_record_name) -%}
  STRUCT< 
{%- for parameter in all_parameters -%}
    {%- set property_name = parameter[0] -%}
    {%- set data_type = parameter[1].lower() -%}
    {%- set how_to_extract_value = get_parameter_type(property_name, data_type)[0] -%}
    {%- set bq_type = get_parameter_type(property_name, data_type)[1] -%}
    {{ property_name }}_{{ data_type }} {{ bq_type }}{{ ", " if not loop.last else "" }}
{%- endfor -%}>(
      {% for parameter in all_parameters -%}
        {%- set property_name = parameter[0] -%}
        {%- set data_type = parameter[1].lower() -%}
        {%- set how_to_extract_value = get_parameter_type(property_name, data_type)[0] -%}
        {%- set bq_type = get_parameter_type(property_name, data_type)[1] -%}
      (SELECT {{ how_to_extract_value }} FROM UNNEST({{ firebase_record_name }}) WHERE key = '{{ property_name }}') {{ ", " if not loop.last else "" }}
    {%- endfor %}
    )
{%- endmacro %}


{# returns an array of [how to extract value, TYPE of said value] #}
{% macro get_parameter_type(parameter_name, data_type) %}
{% set data_type_to_value = {'string' : ['LOWER(value.string_value)', 'STRING' ], 'int':['value.int_value', 'INT64'], 'double':['value.double_value', 'DOUBLE']  }%}
{%- if not data_type in  ['string','int','double']  -%}
{{ exceptions.raise_compiler_error(" data type '" + data_type + "' not supported (only string, int & double are supported) for custom parameter named'" + parameter_name + "'" ) }}
{%- endif %}
{{ return(data_type_to_value[data_type.lower()] ) }}
{% endmacro %}
