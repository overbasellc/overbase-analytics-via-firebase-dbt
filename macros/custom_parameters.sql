{# Array of tuples [
(property_name, overbase_type, rollup_type, extract_transformation, metric_rollup_transformation, struct_field_name, bigquery_type, how_to_extract_from_unnest)] #}
{% macro get_user_property_tuples_all() -%}
    {% set builtin_parameters = [ 
            ("ob_ui_dark_mode", "STRING", "dimension", Undefined, Undefined, Undefined, Undefined)
           ,("ob_ui_font_size", "STRING", "dimension", Undefined, Undefined, Undefined, Undefined)
    ]%}
    {%- set all_parameters = builtin_parameters +  overbase_firebase.flatten_yaml_parameters(var('OVERBASE:CUSTOM_USER_PROPERTIES', [])) -%}
    {%- set all_parameters = overbase_firebase.set_transformation_and_field_name(all_parameters) -%}
    {%- set all_parameters = overbase_firebase.add_extra_types(all_parameters, "analytics") -%}
    {%- do overbase_firebase.validate_parameter_tuples(all_parameters) -%}
    {{ return(all_parameters) }}
{%- endmacro %}


{# Array of tuples [
(property_name, overbase_type, rollup_type, extract_transformation, metric_rollup_transformation, struct_field_name, bigquery_type, how_to_extract_from_unnest)] 
    ('ob_view_name', 'STRING', 'dimension', 'STRING', 'LOWER(value.string_value)', 'ob_view_name_string')
#}
{% macro get_event_parameter_tuples_all() -%}
    {% set builtin_parameters = [ 
            ("ob_view_name", "STRING", "dimension", Undefined, Undefined, Undefined, Undefined)
           ,("ob_view_type", "STRING", "alsoForceNullDimension", Undefined, Undefined, Undefined, Undefined)
           ,("ob_parent_view_name", "STRING", "dimension", Undefined, Undefined, Undefined, Undefined)
           ,("ob_parent_view_type", "STRING", "alsoForceNullDimension", Undefined, Undefined, Undefined, Undefined)
           ,("ob_name", "STRING", "dimension", Undefined, Undefined, Undefined, Undefined)
    ]%}
    {%- set all_parameters = builtin_parameters +  overbase_firebase.flatten_yaml_parameters(var('OVERBASE:CUSTOM_EVENT_PARAMETERS', [])) -%}
    {%- set all_parameters = overbase_firebase.set_transformation_and_field_name(all_parameters) -%}
    {%- set all_parameters = overbase_firebase.add_extra_types(all_parameters, "analytics") -%}
    {%- do overbase_firebase.validate_parameter_tuples(all_parameters) -%}
    {{ return(all_parameters) }}
{%- endmacro %}

{# Array of tuples [
(property_name, overbase_type, rollup_type, extract_transformation, metric_rollup_transformation, struct_field_name, bigquery_type, how_to_extract_from_unnest)] 
    ('ob_view_name', 'STRING', 'dimension', 'STRING', 'LOWER(value.string_value)', 'ob_view_name_string')
#}
{% macro get_crashlytics_custom_key_tuples_all() -%}
    {% set builtin_parameters = [ 
    ]%}
    {%- set all_parameters = builtin_parameters +  overbase_firebase.flatten_yaml_parameters(var('OVERBASE:CUSTOM_CRASHLYTICS_KEYS', [])) -%}
    {%- set all_parameters = overbase_firebase.set_transformation_and_field_name(all_parameters) -%}
    {%- set all_parameters = overbase_firebase.add_extra_types(all_parameters, "crashlytics") -%}
    {%- do overbase_firebase.validate_parameter_tuples(all_parameters) -%}
    {{ return(all_parameters) }}
{%- endmacro %}



{# e.g. [event_parameters.call_id_string (a raw only custom property), event_parameters.quantity_int (a rollup_metric custom property)] #}
{%- macro get_mini_columns_to_ignore_when_rolling_up() -%}
    {%- set eventParamsToIgnoreInGroupBy = overbase_firebase.get_event_parameter_tuples_for_raw_only() + overbase_firebase.get_event_parameter_tuples_for_rollup_metrics() -%}
    {%- set eventParamsToIgnoreInGroupBy = overbase_firebase.list_map_and_add_prefix(eventParamsToIgnoreInGroupBy|map(attribute=5)|list, 'event_parameters.' ) -%}

    {%- set userPropertiesToIgnoreInGroupBy = overbase_firebase.get_user_property_tuples_for_raw_only() + overbase_firebase.get_user_property_tuples_for_rollup_metrics() -%}
    {%- set userPropertiesToIgnoreInGroupBy = overbase_firebase.list_map_and_add_prefix(userPropertiesToIgnoreInGroupBy|map(attribute=5)|list, 'user_properties.' ) -%}

    {%- set miniColumnsToIgnoreInGroupBy = eventParamsToIgnoreInGroupBy + userPropertiesToIgnoreInGroupBy -%}

    {%- set miniColumnExclusions = var('OVERBASE:OB_DIMENSION_TO_EXCLUDE_IN_ROLLUPS', ["geo.city", "geo.metro", "geo.region"]) -%}
    {%- for exclusion in miniColumnExclusions %}
            {{ miniColumnsToIgnoreInGroupBy.append(exclusion) }}
    {% endfor %}

    {{ return(miniColumnsToIgnoreInGroupBy) }}
{%- endmacro -%}

{%- macro get_mini_columns_to_also_force_null_when_rolling_up() -%}
    {%- set eventParamsToNil = overbase_firebase.get_event_parameter_tuples_for_rollup_alsoNullDimensions() -%}
    {%- set eventParamsToNil = overbase_firebase.list_map_and_add_prefix(eventParamsToNil|map(attribute=5)|list, 'event_parameters.' ) -%}

    {%- set userPropertiesToNil = overbase_firebase.get_user_property_tuples_for_rollup_alsoNullDimensions() -%}
    {%- set userPropertiesToNil = overbase_firebase.list_map_and_add_prefix(userPropertiesToNil|map(attribute=5)|list, 'user_properties.' ) -%}

    {%- set miniColumnsToNil = eventParamsToNil + userPropertiesToNil -%}

    {{ return(miniColumnsToNil) }}
{%- endmacro -%}


{# ################################### #}
{# Helper Macros #}
{%- macro get_user_property_tuples_for_rollup_metrics() -%}
{%- set result = overbase_firebase.get_user_property_tuples_all() | selectattr(2, 'equalto', 'metric') | list -%}
{%- do return(result) -%}
{%- endmacro %}

{%- macro get_user_property_tuples_for_rollup_dimensions() -%}
{%- set result = overbase_firebase.get_user_property_tuples_all() | selectattr(2, 'equalto', 'dimension') | list -%}
{%- set result2 = overbase_firebase.get_user_property_tuples_all() | selectattr(2, 'equalto', 'alsoForceNullDimension') | list -%}
{%- do return(result + result2) -%}
{%- endmacro %}

{%- macro get_user_property_tuples_for_rollup_alsoNullDimensions() -%}
{%- set result = overbase_firebase.get_user_property_tuples_all() | selectattr(2, 'equalto', 'alsoForceNullDimension') | list -%}
{%- do return(result) -%}
{%- endmacro %}

{%- macro get_user_property_tuples_for_raw_only() -%}
{%- set result  = overbase_firebase.get_user_property_tuples_all() | selectattr(2, 'equalto', 'raw') | list -%}
{%- set result2 = overbase_firebase.get_user_property_tuples_all() | selectattr(2, 'equalto', '') | list -%}
{%- set result3 = overbase_firebase.get_user_property_tuples_all() | selectattr(2, 'undefined') | list -%}
{%- do return(result + result2 + result3) -%}
{%- endmacro %}


{%- macro get_event_parameter_tuples_for_rollup_metrics() -%}
{%- set result = overbase_firebase.get_event_parameter_tuples_all() | selectattr(2, 'equalto', 'metric') | list -%}
{%- do return(result) -%}
{%- endmacro %}

{%- macro get_event_parameter_tuples_for_rollup_dimensions() -%}
{%- set result = overbase_firebase.get_event_parameter_tuples_all() | selectattr(2, 'equalto', 'dimension') | list -%}
{%- set result2 = overbase_firebase.get_event_parameter_tuples_all() | selectattr(2, 'equalto', 'alsoForceNullDimension') | list -%}
{%- do return(result + result2) -%}
{%- endmacro %}

{%- macro get_event_parameter_tuples_for_rollup_alsoNullDimensions() -%}
{%- set result = overbase_firebase.get_event_parameter_tuples_all() | selectattr(2, 'equalto', 'alsoForceNullDimension') | list -%}
{%- do return(result) -%}
{%- endmacro %}


{%- macro get_event_parameter_tuples_for_raw_only() -%}
{%- set result  = overbase_firebase.get_event_parameter_tuples_all() | selectattr(2, 'equalto', 'raw') | list -%}
{%- set result2 = overbase_firebase.get_event_parameter_tuples_all() | selectattr(2, 'equalto', '') | list -%}
{%- set result3 = overbase_firebase.get_event_parameter_tuples_all() | selectattr(2, 'undefined') | list -%}
{%- do return(result + result2 + result3) -%}
{%- endmacro %}



{# Flatten from YAML array dict [{'property_name': 'foo', 'data_type':'bar', 'rollup_type':'dimension'}] to a simple array
 of tuples [('my_custom_user_prop', 'string', 'dimension', 'extract_transformation', 'metric_rollup_transformation', 'field_name', 'output_data_type')] #}
{% macro flatten_yaml_parameters(custom_array_of_dicts) -%}
    {% set flat_result = [] %}
    {% for dict in custom_array_of_dicts %}
        {{ flat_result.append((dict['property_name'], dict['data_type'], dict['rollup_type'], dict['extract_transformation'], dict['metric_rollup_transformation'], dict['field_name'], dict['output_data_type'])) }}
    {% endfor %}
    {{ return(flat_result) }}
{% endmacro %}

{% macro set_transformation_and_field_name(parameter_tuples) -%}
    {%- set result = [] -%}
    {%- for tuple in parameter_tuples -%}
        {%- set property_name = tuple[0] -%}
        {%- set data_type = tuple[1] -%}
        {%- set rollup_type = tuple[2] -%}

        {%- set extract_transformation = tuple[3] -%}
        {%- if extract_transformation is not defined -%}
            {%- set extract_transformation = "##" -%}
        {%- endif -%}

        {%- set metric_rollup_transformation = tuple[4] -%}
        {%- if metric_rollup_transformation is not defined -%}
            {%- if rollup_type == "metric" -%}
                {%- set metric_rollup_transformation = "SUM(##)" -%}
            {%- endif -%}
        {%- endif -%}

        {%- set field_name = tuple[5] -%}
        {%- if field_name is not defined -%}
            {%- set field_name = property_name ~ "_" ~ data_type.lower() -%}
        {%- endif -%}
        {%- set bq_type = tuple[6] -%}

        {%- set _ = result.append((property_name, data_type, rollup_type, extract_transformation, metric_rollup_transformation, field_name, bq_type) ) -%}
    {%- endfor -%}
    {{ return(result) }}
{% endmacro %}

{% macro add_extra_types(parameter_tuples, analyticsOrCrashlytics) -%}
    {%- set result = [] -%}
    {%- for tuple in parameter_tuples -%}
        {%- if analyticsOrCrashlytics == "analytics" -%}
            {%- set bqTypeAndHowToExtractTuple = overbase_firebase.get_extra_parameter_types(tuple[0], tuple[1].lower()) -%}
        {%- else -%}
            {%- set bqTypeAndHowToExtractTuple = overbase_firebase.get_extra_parameter_types_crashlytics(tuple[0], tuple[1].lower()) -%}
        {%- endif -%}
        {%- if tuple[6] is not defined -%}
            {%- set _ = result.append((tuple[0],tuple[1], tuple[2], tuple[3], tuple[4], tuple[5], bqTypeAndHowToExtractTuple[0], bqTypeAndHowToExtractTuple[1])) -%}
        {%- else -%}
            {%- set _ = result.append((tuple[0],tuple[1], tuple[2], tuple[3], tuple[4], tuple[5], tuple[6], bqTypeAndHowToExtractTuple[1])) -%}
        {%- endif -%}        
    {%- endfor -%}
    {{ return(result) }}
{% endmacro %}



{# returns an tuple of (TYPE of said value, how to extract value) 
    ('STRING', 'LOWER(value.string_value)')
#}
{% macro get_extra_parameter_types(parameter_name, data_type) %}
    {% set data_type_to_value = {'string' : ['STRING', 'LOWER(value.string_value)'], 'int':['INT64', 'value.int_value'], 'double':['DOUBLE', 'value.double_value']  }%}
    {%- if not data_type in  ['string','int','double']  -%}
        {{ exceptions.raise_compiler_error(" data type '" + data_type + "' not supported (only string, int & double are supported) for custom parameter named'" + parameter_name + "'" ) }}
    {%- endif %}
    {%- set res = data_type_to_value[data_type.lower()] %}
    {{ return( (res[0], res[1]) ) }}
{% endmacro %}

{# returns an tuple of (TYPE of said value, how to extract value) 
    ('STRING', 'LOWER(value.string_value)')
#}
{% macro get_extra_parameter_types_crashlytics(parameter_name, data_type) %}
    {% set data_type_to_value = {'string' : ['STRING', 'LOWER(value)'] }%}
    {%- if not data_type in  ['string']  -%}
        {{ exceptions.raise_compiler_error(" data type '" + data_type + "' not supported (only string is supported) for custom crashlytics key named'" + parameter_name + "'" ) }}
    {%- endif %}
    {%- set res = data_type_to_value[data_type.lower()] %}
    {{ return( (res[0], res[1]) ) }}
{% endmacro %}


{%- macro validate_parameter_tuples(tuples) -%}
    {%- for tuple in tuples -%}
        {%- set rollupType = tuple[2] -%}
        {%- if rollupType|length > 0  and rollupType not in ['raw', 'dimension', 'alsoForceNullDimension', 'metric'] -%}
                {{ exceptions.raise_compiler_error(" 'rollup_type' '" + rollupType + "' not supported (only 'raw', 'dimension', 'alsoForceNullDimension', 'metric' supported). Looking at parameter:" + tuple[0]) }}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}