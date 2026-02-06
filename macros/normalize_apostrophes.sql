{% macro normalize_apostrophes(col) %}
REGEXP_REPLACE(
  {{ col }},
  '[‘’]',
  ''''
)
{% endmacro %}
