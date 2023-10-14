{% macro generate_struct_for_user_properties() -%}
    {{ overbase_firebase.generate_struct(overbase_firebase.get_user_property_tuples(), 'user_properties') }}
{%- endmacro%}

{% macro generate_struct_for_event_parameters() -%}
    {{ overbase_firebase.generate_struct(overbase_firebase.get_event_parameter_tuples_all(), 'event_params') }}
{%- endmacro%}


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
    {%- set data_type = parameter[1] -%}
    {%- set is_metric = parameter[2] -%}
    {%- set bq_type = parameter[3] -%}
    {%- set how_to_extract_value = parameter[4] -%}
    {%- set struct_field_name = parameter[5] -%}
    {{ struct_field_name }} {{ bq_type }}{{ ", " if not loop.last else "" }}
{%- endfor -%}>(
      {% for parameter in all_parameters -%}
        {%- set property_name = parameter[0] -%}
        {%- set data_type = parameter[1] -%}
        {%- set is_metric = parameter[2] -%}
        {%- set bq_type = parameter[3] -%}
        {%- set how_to_extract_value = parameter[4] -%}
           (SELECT {{ how_to_extract_value }} FROM UNNEST({{ firebase_record_name }}) WHERE key = '{{ property_name }}') {{ ", " if not loop.last else "" }}
    {%- endfor %}
    )
{%- endmacro %}

