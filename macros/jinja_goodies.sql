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

{# So event_name ["foo", "bar"] becomes the string: event_name IN ("foo", "bar"). 
If the list is empty, it returns just True
 #}
{%- macro makeListIntoSQLInFilter(sqlField, myList) -%}
  {%- if myList | length > 0 -%}
    {{ sqlField }} IN {{ tojson(myList).replace("[", "(").replace("]", ")") }}
  {%- else -%}
    True
  {%- endif -%}
{%- endmacro -%}



{# So event_name ["foo", "bar"] becomes the string: event_name IN ("foo", "bar"). 
If the list is empty, it returns just True
 #}
{%- macro flatten_list_of_lists(myListOfLists) -%}
  {%- set res = [] %}
  {%- for myList in myListOfLists -%}
    {%- for elem in myList -%}
      {%- set _ = res.append(elem) -%}    
    {%- endfor -%}
  {%- endfor -%}
  {{ return(res) }}
{%- endmacro -%}