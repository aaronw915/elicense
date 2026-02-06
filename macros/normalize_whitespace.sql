{% macro normalize_whitespace(col) %}
REGEXP_REPLACE(
  {{ col }},
  '\u00A0',
  ' '
)
{% endmacro %}
