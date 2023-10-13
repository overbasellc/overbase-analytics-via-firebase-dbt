{# STRUCT<minus_12 DATE , minus_11 DATE , minus_10 DATE , minus_9 DATE , minus_8 DATE , minus_7 DATE , minus_6 DATE , minus_5 DATE , minus_4 DATE , minus_3 DATE , minus_2 DATE , minus_1 DATE , plus_1 DATE , plus_2 DATE , plus_3 DATE , plus_4 DATE , plus_5 DATE , plus_6 DATE , plus_7 DATE , plus_8 DATE , plus_9 DATE , plus_10 DATE , plus_11 DATE , plus_12 DATE >(
            DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-12:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-11:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-10:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-09:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-08:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-07:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-06:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-05:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-04:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-03:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-02:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '-01:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+01:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+02:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+03:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+04:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+05:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+06:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+07:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+08:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+09:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+10:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+11:00')) , DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), '+12:00')) 
)
#}
{%- macro generate_date_timezone_tuple(tsField) -%}
    STRUCT<
    {%- for n in range(-12, 13) -%}
        {%- if n < 0 -%}
            minus_{{ n|abs }} DATE {{ ", " if not loop.last else "" }}
            {%- elif n > 0 -%}
            plus_{{ n|abs }} DATE {{ ", " if not loop.last else "" }}
        {%- endif -%}
    {% endfor -%}>(
    {% for n in range(-12, 13) -%}
        {%- if n < 0 -%}
            DATE(DATETIME({{ tsField }}, '-{{ '%02d' % n|abs }}:00')) {{ ", " if not loop.last else "" }}
            {%- elif n > 0 -%}
            DATE(DATETIME({{ tsField }}, '+{{ '%02d' % n|abs }}:00')) {{ ", " if not loop.last else "" }}
        {%- endif -%}
    {% endfor %}
    )
{%- endmacro -%}