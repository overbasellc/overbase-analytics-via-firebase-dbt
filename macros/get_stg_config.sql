{% macro get_stg_config() %}

    {{
        return(
            {
                "project": var("overbase_project", target.project),  
                "dataset": var("overbase_dataset", "placeholder_dataset"),
                
            }
        )
    }}      ## todo: replace bigquery specific target.project with adapter function

{% endmacro %}
