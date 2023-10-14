{%- macro list_map_and_add_prefix(list, prefix) -%}
{%- set res = [] %}
{%- for elem in list -%}
    {%- set _ = res.append(prefix ~ elem) -%}
{%- endfor -%}
{{ return(res) }}
{%- endmacro -%}

{%- macro list_map_and_add_suffix(list, suffix) -%}
{%- set res = [] %}
{%- for elem in list -%}
    {%- set _ = res.append(elem ~ suffix) -%}
{%- endfor -%}
{{ return(res) }}
{%- endmacro -%}