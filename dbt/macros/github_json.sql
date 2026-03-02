{% macro raw_payload_col() %}
  {{ return(adapter.quote(var('raw_payload_col', 'payload'))) }}
{% endmacro %}

{% macro raw_ingested_at_col() %}
  {{ return(adapter.quote(var('raw_ingested_at_col', 'ingested_at'))) }}
{% endmacro %}

{% macro json_scalar(json_expr, path) %}
  JSON_EXTRACT_SCALAR({{ json_expr }}, '{{ path }}')
{% endmacro %}

{% macro parse_github_ts(ts_expr) %}
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', {{ ts_expr }})
{% endmacro %}