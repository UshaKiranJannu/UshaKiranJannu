{#
  epoch_ms_to_timestamp(col)
  ───────────────────────────
  Converts an epoch-millisecond column to a proper timestamp.

  DuckDB:    epoch_ms(col)          — native function
  Snowflake: TO_TIMESTAMP_NTZ(col / 1000)

  Usage in models:
      {{ epoch_ms_to_timestamp('timestamp') }}
#}

{% macro epoch_ms_to_timestamp(col) %}
  {% if target.type == 'duckdb' %}
    epoch_ms({{ col }})
  {% else %}
    TO_TIMESTAMP_NTZ({{ col }} / 1000)
  {% endif %}
{% endmacro %}
