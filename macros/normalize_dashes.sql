{% macro normalize_dashes(col) %}
REGEXP_REPLACE(
  {{ col }},
  '[–—]',
  '-'
)
{% endmacro %}
