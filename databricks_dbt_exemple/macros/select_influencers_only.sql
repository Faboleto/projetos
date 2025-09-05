{% macro select_influencers_only(table_name) %}
  {% set metadata_columns = ['_', '_airbyte_raw_id', '_airbyte_extracted_at','_airbyte_meta','_airbyte_generation_id'] %}
  {% set columns = adapter.get_columns_in_relation(source('db', table_name)) %}
  {% set influencer_columns = [] %}

  {% for col in columns %}
    {% if col.name not in metadata_columns %}
      {% do influencer_columns.append(col.name) %}
    {% endif %}
  {% endfor %}

  SELECT
    to_date(_) as reference_date,
    {% for col in influencer_columns %}
      try_cast(replace({{ col }}, ',', '.') as double) as {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}
  FROM {{ source('db', table_name) }}
{% endmacro %}