{% set config = get_stg_config() %}


select 1 as test
from `{{ config["project"] }}.{{ config["dataset"] }}.events_intraday_*`


