{# Array of tuples [(property_name, overbase_type, is_metric (Bool), bigquery_type, how_to_extract_from_unnest, struct_field_name)] #}
{% macro get_user_property_tuples() -%}
    {% set builtin_parameters = [ 
            ("ob_ui_dark_mode", "STRING", False)
           ,("ob_ui_font_size", "STRING", False)
    ]%}
    {% set all_parameters = builtin_parameters +  overbase_firebase.flatten_yaml_parameters(var('OVERBASE:CUSTOM_USER_PROPERTIES', [])) %}
    {% set all_complete_parameters = overbase_firebase.add_extra_types(all_parameters) %}
    {{ return(all_complete_parameters) }}
{%- endmacro %}

{# Array of tuples [(property_name, overbase_type, is_metric (Bool), bigquery_type, how_to_extract_from_unnest, struct_field_name)] 
    ('ob_view_name', 'STRING', False, 'STRING', 'LOWER(value.string_value)', 'ob_view_name_string')
#}
{% macro get_event_parameter_tuples_all() -%}
    {% set builtin_parameters = [ 
            ("ob_view_name", "STRING", False)
           ,("ob_view_type", "STRING", False)
           ,("ob_parent_view_name", "STRING", False)
           ,("ob_parent_view_type", "STRING", False)
           ,("ob_name", "STRING", False)
    ]%}
    {% set all_parameters = builtin_parameters +  overbase_firebase.flatten_yaml_parameters(var('OVERBASE:CUSTOM_EVENT_PARAMETERS', [])) %}
    {% set all_complete_parameters = overbase_firebase.add_extra_types(all_parameters) %}
    {{ return(all_complete_parameters) }}
{%- endmacro %}


{%- macro get_event_parameter_tuples_metrics_only() -%}
{%- set result = [] -%}
{%- for tuple in overbase_firebase.get_event_parameter_tuples_all() -%}
    {%- if tuple[2] == True -%}
        {%- set _ = result.append(tuple) -%}
    {% endif -%}
{%- endfor -%}
{{ return(result) }}
{%- endmacro %}

{%- macro get_event_parameter_tuples_dimensions_only() -%}
{%- set result = [] -%}
{%- for tuple in overbase_firebase.get_event_parameter_tuples_all() -%}
    {%- if tuple[2] == False -%}
        {%- set _ = result.append(tuple) -%}
    {% endif -%}
{%- endfor -%}
{{ return(result) }}
{%- endmacro %}



{# Flatten from YAML array dict [{'property_name': 'foo', 'data_type':'bar'}] to a simple array of tuples [('my_custom_user_prop', 'string')] #}
{% macro flatten_yaml_parameters(custom_array_of_dicts) -%}
    {% set flat_result = [] %}
    {% for dict in custom_array_of_dicts %}
        {{ flat_result.append((dict['property_name'], dict['data_type'], dict['is_metric'])) }}
    {% endfor %}
    {{ return(flat_result) }}
{% endmacro %}

{% macro add_extra_types(parameter_tuples) -%}
    {% set result = [] %}
    {% for tuple in parameter_tuples %}
        {{ result.append((tuple + overbase_firebase.get_extra_parameter_types(tuple[0], tuple[1].lower()))) }}
    {% endfor %}
    {{ return(result) }}
{% endmacro %}


{# returns an tuple of (TYPE of said value, how to extract value, struct field name) 
    ('STRING', 'LOWER(value.string_value)', $parameter_name ~ '_' ~ $data_type)
#}
{% macro get_extra_parameter_types(parameter_name, data_type) %}
    {% set data_type_to_value = {'string' : ['STRING', 'LOWER(value.string_value)'], 'int':['INT64', 'value.int_value'], 'double':['DOUBLE', 'value.double_value']  }%}
    {%- if not data_type in  ['string','int','double']  -%}
        {{ exceptions.raise_compiler_error(" data type '" + data_type + "' not supported (only string, int & double are supported) for custom parameter named'" + parameter_name + "'" ) }}
    {%- endif %}
    {%- set res = data_type_to_value[data_type.lower()] + [ parameter_name ~ "_" ~ data_type.lower() ] %}
    {{ return( (res[0], res[1], res[2] ) ) }}
{% endmacro %}


