{% macro get_stg_config() %}

    {{
        return(
            {
                "project": var("overbase_project", "placeholder_project"),
                "dataset": var("overbase_dataset", "placeholder_dataset"),
            }
        )
    }}

{% endmacro %}
