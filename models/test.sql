
{{ get_event_parameter_tuples_all() | map(attribute=0) | list }}


{{ get_event_parameter_tuples_for_rollup_dimensions() | map(attribute=0) | list }}



{{ get_event_parameter_tuples_for_rollup_dimensions_to_ignore() | map(attribute=0) | list }}
