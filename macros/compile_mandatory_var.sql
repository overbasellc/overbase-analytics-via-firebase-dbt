-- a variable that must be overwritten when OB Analytics via FB is used in another DBT project
-- the default value is meant only to be used within this project, as test values 
{% macro compile_mandatory_var(variable_name, default_value_in_ob) %}

{% if var(variable_name, default_value_in_ob) == default_value_in_ob and env_var('DBT_INSIDE_OVERBASE','')|length == 0 %}
   {{ exceptions.raise_compiler_error("Variable '%s' must be overwritten inside your dbt_project.yml. Check the Overbase documentation to see all mandatory variables" % variable_name) }}
{% endif %}


{% endmacro %}

