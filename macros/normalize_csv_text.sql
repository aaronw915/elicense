{% macro normalize_csv_text(col) %}
REGEXP_REPLACE(
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          {{ col }},
          '[‘’\u0091\u0092]', ''''
        ),
        '[“”\u0093\u0094]', '"'
      ),
      '[–—\u0096\u0097]', '-'
    ),
    '²', '2'
  ),
  '[\u00A0\t\r\n]+', ' '
)
{% endmacro %}
