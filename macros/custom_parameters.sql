{# Array of dicts
  key_name, data_type, rollup_type, extract_transformation, metric_rollup_transformation, struct_field_name, output_data_type
  event_name_filter, force_null_dimension_event_name_filter
#}
{% macro get_user_property_tuples_all() -%}
    {% set builtin_parameters = [ 
            {"key_name":"ob_ui_dark_mode", "data_type":"STRING", "rollup_type": "dimension"}
           ,{"key_name":"ob_ui_font_size", "data_type":"STRING", "rollup_type": "dimension"}
    ]%}
    {%- set all_parameters = builtin_parameters +  var('OVERBASE:CUSTOM_USER_PROPERTIES', []) -%}
    {%- set all_parameters = overbase_firebase.set_transformation_and_field_name(all_parameters, "analytics") -%}
    {%- do overbase_firebase.validate_parameter_tuples(all_parameters) -%}
    {{ return(all_parameters) }}
{%- endmacro %}


{# Array of tuples [
(key_name, overbase_type, rollup_type, extract_transformation, metric_rollup_transformation, struct_field_name, event_name_filter, null_dimension_event_name_filter, bigquery_type, how_to_extract_from_unnest)] 
    ('ob_view_name', 'STRING', 'dimension', 'STRING', 'ob_ui_view_shown', 'LOWER(value.string_value)', 'ob_view_name_string')
#}
{% macro get_event_parameter_tuples_all() -%}
    {# ob_ui_view_shown & ob_ui_button_tapped #}
    {%- set uiEventNameFilters = ["ob_ui_view_shown", "ob_ui_button_tapped", "ui_view_shown", "ui_button_tapped"] -%}
    {%- set builtin_parameters = [ 
            {"key_name":"view_name", "data_type":"STRING", "rollup_type":"dimension"} 
           ,{"key_name":"view_type", "data_type":"STRING", "rollup_type":"alsoForceNullDimension", "force_null_dimension_event_name_filter": uiEventNameFilters}
           ,{"key_name":"parent_view_name", "data_type":"STRING", "rollup_type":"dimension"}
           ,{"key_name":"parent_view_type", "data_type":"STRING", "rollup_type":"alsoForceNullDimension",  "force_null_dimension_event_name_filter": uiEventNameFilters}
    ]%}
    {#  ob_ui_button_tapped #}
    {% set builtin_parameters = builtin_parameters + [
            {"key_name":"button_name", "data_type":"STRING", "rollup_type":"dimension"} 
           ,{"key_name":"button_extra", "data_type":"STRING", "rollup_type":"dimension"} 
    ]%}
    {# ob_error_server_% #}
    {% set builtin_parameters = builtin_parameters + [
            {"key_name":"domain", "data_type":"STRING", "rollup_type":"dimension"}  
           ,{"key_name":"path", "data_type":"STRING", "rollup_type":"dimension"}
           ,{"key_name":"status_code", "data_type":"INT", "rollup_type":"dimension"}
           ,{"key_name":"extra", "data_type":"STRING", "rollup_type":"dimension"}
    ]%}
    {%- set all_parameters = builtin_parameters +  var('OVERBASE:CUSTOM_EVENT_PARAMETERS', []) -%}
    {%- set all_parameters = overbase_firebase.set_transformation_and_field_name(all_parameters, "analytics") -%}
    {%- do overbase_firebase.validate_parameter_tuples(all_parameters) -%}
    {{ return(all_parameters) }}
{%- endmacro %}

{# Array of tuples [
(key_name, overbase_type, rollup_type, extract_transformation, metric_rollup_transformation, struct_field_name, event_name_filter, null_dimension_event_name_filter, bigquery_type, how_to_extract_from_unnest)] 
    ('ob_view_name', 'STRING', 'dimension', 'STRING', 'LOWER(value.string_value)', 'ob_view_name_string')
#}
{% macro get_crashlytics_custom_key_tuples_all() -%}
    {% set builtin_parameters = [ 
    ]%}
    {%- set all_parameters = builtin_parameters +  var('OVERBASE:CUSTOM_CRASHLYTICS_KEYS', []) -%}
    {%- set all_parameters = overbase_firebase.set_transformation_and_field_name(all_parameters, "crashlytics") -%}
    {%- do overbase_firebase.validate_parameter_tuples(all_parameters) -%}
    {{ return(all_parameters) }}
{%- endmacro %}



{# e.g. [event_parameters.call_id_string (a raw only custom property), event_parameters.quantity_int (a rollup_metric custom property)] #}
{%- macro get_mini_columns_to_ignore_when_rolling_up() -%}
    {%- set eventParamsToIgnoreInGroupBy = overbase_firebase.get_event_parameter_tuples_for_raw_only() + overbase_firebase.get_event_parameter_tuples_for_rollup_metrics() -%}
    {%- set eventParamsToIgnoreInGroupBy = overbase_firebase.list_map_and_add_prefix(eventParamsToIgnoreInGroupBy|map(attribute='struct_field_name')|list, 'event_parameters.' ) -%}

    {%- set userPropertiesToIgnoreInGroupBy = overbase_firebase.get_user_property_tuples_for_raw_only() + overbase_firebase.get_user_property_tuples_for_rollup_metrics() -%}
    {%- set userPropertiesToIgnoreInGroupBy = overbase_firebase.list_map_and_add_prefix(userPropertiesToIgnoreInGroupBy|map(attribute='struct_field_name')|list, 'user_properties.' ) -%}

    {%- set miniColumnsToIgnoreInGroupBy = eventParamsToIgnoreInGroupBy + userPropertiesToIgnoreInGroupBy -%}

    {%- set miniColumnExclusions = var('OVERBASE:OB_DIMENSION_TO_EXCLUDE_IN_ROLLUPS', ["geo.city", "geo.metro", "geo.region"]) -%}
    {%- for exclusion in miniColumnExclusions %}
            {{ miniColumnsToIgnoreInGroupBy.append(exclusion) }}
    {% endfor %}

    {{ return(miniColumnsToIgnoreInGroupBy) }}
{%- endmacro -%}

{%- macro get_mini_columns_to_also_force_null_when_rolling_up() -%}
    {%- set eventParamsToNil = overbase_firebase.get_event_parameter_tuples_for_rollup_alsoNullDimensions() -%}
    {%- set eventParamsToNil = overbase_firebase.list_map_and_add_prefix(eventParamsToNil|map(attribute='struct_field_name')|list, 'event_parameters.' ) -%}

    {%- set userPropertiesToNil = overbase_firebase.get_user_property_tuples_for_rollup_alsoNullDimensions() -%}
    {%- set userPropertiesToNil = overbase_firebase.list_map_and_add_prefix(userPropertiesToNil|map(attribute='struct_field_name')|list, 'user_properties.' ) -%}

    {%- set miniColumnsToNil = eventParamsToNil + userPropertiesToNil -%}

    {{ return(miniColumnsToNil) }}
{%- endmacro -%}


{# ################################### #}
{# Helper Macros #}
{%- macro get_user_property_tuples_for_rollup_metrics() -%}
{%- set result = overbase_firebase.get_user_property_tuples_all() | selectattr('rollup_type', 'equalto', 'metric') | list -%}
{%- do return(result) -%}
{%- endmacro %}

{%- macro get_user_property_tuples_for_rollup_dimensions() -%}
{%- set result = overbase_firebase.get_user_property_tuples_all() | selectattr('rollup_type', 'equalto', 'dimension') | list -%}
{%- set result2 = overbase_firebase.get_user_property_tuples_all() | selectattr('rollup_type', 'equalto', 'alsoForceNullDimension') | list -%}
{%- do return(result + result2) -%}
{%- endmacro %}

{%- macro get_user_property_tuples_for_rollup_alsoNullDimensions() -%}
{%- set result = overbase_firebase.get_user_property_tuples_all() | selectattr('rollup_type', 'equalto', 'alsoForceNullDimension') | list -%}
{%- do return(result) -%}
{%- endmacro %}

{%- macro get_user_property_tuples_for_raw_only() -%}
{%- set result  = overbase_firebase.get_user_property_tuples_all() | selectattr('rollup_type', 'equalto', 'raw') | list -%}
{%- set result2 = overbase_firebase.get_user_property_tuples_all() | selectattr('rollup_type', 'equalto', '') | list -%}
{%- set result3 = overbase_firebase.get_user_property_tuples_all() | selectattr('rollup_type', 'undefined') | list -%}
{%- do return(result + result2 + result3) -%}
{%- endmacro %}


{%- macro get_event_parameter_tuples_for_rollup_metrics() -%}
{%- set result = overbase_firebase.get_event_parameter_tuples_all() | selectattr('rollup_type', 'equalto', 'metric') | list -%}
{%- do return(result) -%}
{%- endmacro %}

{%- macro get_event_parameter_tuples_for_rollup_dimensions() -%}
{%- set result = overbase_firebase.get_event_parameter_tuples_all() | selectattr('rollup_type', 'equalto', 'dimension') | list -%}
{%- set result2 = overbase_firebase.get_event_parameter_tuples_all() | selectattr('rollup_type', 'equalto', 'alsoForceNullDimension') | list -%}
{%- do return(result + result2) -%}
{%- endmacro %}

{%- macro get_event_parameter_tuples_for_rollup_alsoNullDimensions() -%}
{%- set result = overbase_firebase.get_event_parameter_tuples_all() | selectattr('rollup_type', 'equalto', 'alsoForceNullDimension') | list -%}
{%- do return(result) -%}
{%- endmacro %}

{%- macro get_event_parameter_tuples_for_raw_only() -%}
{%- set result  = overbase_firebase.get_event_parameter_tuples_all() | selectattr('rollup_type', 'equalto', 'raw') | list -%}
{%- set result2 = overbase_firebase.get_event_parameter_tuples_all() | selectattr('rollup_type', 'equalto', '') | list -%}
{%- set result3 = overbase_firebase.get_event_parameter_tuples_all() | selectattr('rollup_type', 'undefined') | list -%}
{%- do return(result + result2 + result3) -%}
{%- endmacro %}



{% macro set_transformation_and_field_name(parameterDicts, analyticsOrCrashlytics) -%}
    {%- set result = [] -%}
    {%- for parameterDict in parameterDicts -%}
        {%- if analyticsOrCrashlytics == "analytics" -%}
            {%- set bqTypeAndHowToExtractTuple = overbase_firebase.get_extra_parameter_types(parameterDict['key_name'], parameterDict['data_type'].lower()) -%}
        {%- else -%}
            {%- set bqTypeAndHowToExtractTuple = overbase_firebase.get_extra_parameter_types_crashlytics(parameterDict['key_name'], parameterDict['data_type'].lower()) -%}
        {%- endif -%}

        {%- set key_name = parameterDict['key_name'] -%}
        {%- set data_type = parameterDict['data_type'] -%}
        {%- set rollup_type = parameterDict['rollup_type'] -%}

        {%- set extract_transformation = parameterDict['extract_transformation'] -%}
        {%- if extract_transformation is not defined -%}
            {%- set extract_transformation = "##" -%}
        {%- endif -%}
        {%- set extract_transformation = extract_transformation | replace ("##", bqTypeAndHowToExtractTuple[1]) -%}


        {%- set metric_rollup_transformation = parameterDict['metric_rollup_transformation'] -%}
        {%- if metric_rollup_transformation is not defined -%}
            {%- if rollup_type == "metric" -%}
                {%- set metric_rollup_transformation = "SUM(##)" -%}
            {%- endif -%}
        {%- endif -%}

        {%- set struct_field_name = parameterDict['struct_field_name'] -%}
        {%- if struct_field_name is not defined -%}
            {%- set struct_field_name = key_name ~ "_" ~ data_type.lower() -%}
        {%- endif -%}
        {%- set output_data_type = parameterDict['output_data_type'] -%}
        {%- if output_data_type is not defined -%}
            {%- set output_data_type = bqTypeAndHowToExtractTuple[0] -%}
        {%- endif -%}

        {%- set event_name_filter = parameterDict['event_name_filter'] -%}
        {%- if event_name_filter is not defined -%}
            {%- set event_name_filter = [] -%}
        {%- endif -%}

        {%- set force_null_dimension_event_name_filter = parameterDict['force_null_dimension_event_name_filter'] -%}
        {%- if force_null_dimension_event_name_filter is not defined -%}
            {%- set force_null_dimension_event_name_filter = [] -%}
        {%- endif -%}
        {%- if rollup_type == 'alsoForceNullDimension' and (force_null_dimension_event_name_filter | length) == 0 -%}
            {{ exceptions.raise_compiler_error("rollupType=alsoForceNullDimension also requires a 'force_null_dimension_event_name_filter'") }}
        {%- endif -%}


        {%- set _ = result.append({"key_name": key_name,
                                   "data_type": data_type,
                                   "rollup_type": rollup_type,
                                   "extract_transformation": extract_transformation, 
                                   "metric_rollup_transformation": metric_rollup_transformation, 
                                   "struct_field_name": struct_field_name,
                                   "output_data_type": output_data_type,
                                   "event_name_filter": event_name_filter,
                                   "force_null_dimension_event_name_filter": force_null_dimension_event_name_filter
                            }) -%}
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
        {%- set rollupType = tuple['rollup_type'] -%}
        {%- if rollupType|length > 0  and rollupType not in ['raw', 'dimension', 'alsoForceNullDimension', 'metric'] -%}
                {{ exceptions.raise_compiler_error(" 'rollup_type' '" + rollupType + "' not supported (only 'raw', 'dimension', 'alsoForceNullDimension', 'metric' supported). Looking at parameter:" + tuple['key_name']) }}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}