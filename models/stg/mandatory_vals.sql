
{{ config(materialized='table', sort='timestamp', dist='user_id') }}

{% if var("OVERBASE:FIREBASE_DATASET_ID") == 'overbase' and env_var('DBT_INSIDE_OVERBASE','')|length == 0 %}
   {{ exceptions.raise_compiler_error("Invalid variable. Got: overbase_project" ~ number) }}
{% endif %}