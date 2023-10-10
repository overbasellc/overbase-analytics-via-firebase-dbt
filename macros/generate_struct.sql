{% macro get_overbase_builtin_properties(property_source) %}
    {%- if  property_source == 'user_properties' -%}
        {% set builtin_properties = [{'property_name': 'user_property', 'data_type': 'string'}] %}
    {%- elif property_source == 'event_parameters' -%}
        {% set builtin_properties = [{'property_name': 'view_name', 'data_type': 'string'}] %}
    {%- endif %}
{{ return(builtin_properties) }}
{%- endmacro %}

{% macro get_parameter_type(data_type) %}
{% set data_type_to_value = {'string' : ['lower(value.string_value)', 'STRING', '' ], 'int':['value.int_value', 'INT64', '' ], 'double':['value.double_value', 'DOUBLE', '' ]  }%}
{{ return(data_type_to_value[data_type] ) }}
{% endmacro %}

{% macro generate_struct(parameters_in,property_source) -%}
{% set parent_list = get_overbase_builtin_properties(property_source) + parameters_in  %}
{%- for parameter in parameters_in -%}
{%- if not parameter['data_type'] in  ['string','int','double']  -%}
{{ exceptions.raise_compiler_error(" data type '" + parameter['data_type'] + "' not supported for custom parameter '" + parameter['property_name'] + "'" ) }}
{%- endif %}
{%- endfor -%}
    {{ return(adapter.dispatch('generate_struct', 'overbase_firebase')(parent_list)) }}
{%- endmacro %}

{% macro bigquery__generate_struct(parameters_in) -%}
STRUCT< 
{%- for parameter in parameters_in -%}
{{ parameter['property_name'] }}_{{ parameter['data_type'] }} {{ get_parameter_type(parameter['data_type'])[1] }} {{ ", " if not loop.last else "" }}
{%- endfor -%}>(
{% for parameter in parameters_in -%}
(SELECT {{ get_parameter_type(parameter['data_type'])[0] }} from UNNEST(user_properties) WHERE key in ('{{ parameter['property_name'] }}')) {{ ", " if not loop.last else "" }}
{%- endfor -%}
)
{%- endmacro %}

{{ generate_struct(var('OVERBASE:CUSTOM_USER_PROPERTIES')) }}