{# STRUCT<minus_12 DATE , minus_11 DATE , minus_10 DATE , minus_9 DATE , minus_8 DATE , minus_7 DATE , minus_6 DATE , minus_5 DATE , minus_4 DATE , minus_3 DATE , minus_2 DATE , minus_1 DATE , plus_1 DATE , plus_2 DATE , plus_3 DATE , plus_4 DATE , plus_5 DATE , plus_6 DATE , plus_7 DATE , plus_8 DATE , plus_9 DATE , plus_10 DATE , plus_11 DATE , plus_12 DATE >(
     DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-12:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-11:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-10:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-09:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-08:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-07:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-06:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-05:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-04:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-03:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-02:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-01:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+01:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+02:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+03:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+04:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+05:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+06:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+07:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+08:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+09:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+10:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+11:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+12:00')) 
#}
{%- macro generate_date_timezone_struct(tsField) -%}
    STRUCT<
    {%- for timezoneTuple in overbase_firebase.generate_date_timezone_tuple(tsField) -%}
            {{ timezoneTuple[0] }} DATE {{ ", " if not loop.last else "" }}
    {%- endfor -%}>(
    {% for timezoneTuple in overbase_firebase.generate_date_timezone_tuple(tsField) -%}
            {{ timezoneTuple[1] }}  {{ ", " if not loop.last else "" }}
    {%- endfor -%}
    )
{% endmacro -%}

{# [(struct_field_name, extratction)]
    e.g. [('minus_12', "DATE(DATETIME($tsField), '12:00'))" )]
 #}
{%- macro generate_date_timezone_tuple(tsField) -%}
    {%- set res = [] -%}
    {%- for n in range(-12, 13) -%}
        {%- if n < 0 -%}
            {%- set _ = res.append( ( 'minus_' ~ n|abs, "DATE(DATETIME(" ~ tsField ~ ", '-" ~   '%02d' % n|abs ~ ":00'))" ) ) -%}
            {%- elif n > 0 -%}
            {%- set _ = res.append( ( 'plus_'  ~ n|abs, "DATE(DATETIME(" ~ tsField ~ ", '+" ~   '%02d' % n|abs ~ ":00'))" ) ) -%}
        {%- endif -%}
    {% endfor -%}>
    {{ return(res) }}
{%- endmacro -%}