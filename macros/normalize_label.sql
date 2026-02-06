{% macro normalize_label(col) %}
REGEXP_REPLACE(
  REGEXP_REPLACE(
    {{ col }},
    '[–—]',
    '-'
  ),
  '[‘’]',
  ''''
)
{% endmacro %}
