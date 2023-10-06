{% set config = get_stg_config() %}


select 1 as test
from `{{ config["project"] }}.{{ config["dataset"] }}.events_intraday_*`
{% if target.name == 'default' %} --TODO: what's the target name of deployed jobs?
where _TABLE_SUFFIX >=  --create macro + adapter
{% endif %}

--target_name: {{target.name}}
--{{config}}