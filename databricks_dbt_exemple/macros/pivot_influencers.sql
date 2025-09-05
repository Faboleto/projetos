{% macro pivot_influencers(source_relation, date_column) %}
  
  {%- set cols = adapter.get_columns_in_relation(source_relation) -%}
  
  {%- set influencer_cols = [] -%}
  {%- for col in cols -%}
    {%- if col.name != date_column -%}
      {%- do influencer_cols.append(col.name) -%}
    {%- endif -%}
  {%- endfor -%}

  select
    {{ date_column }},
    font,
    cost
  from {{ source_relation }}
  lateral view stack(
    {{ influencer_cols | length }},
    {%- for col in influencer_cols %}
      '{{ col }}', {{ col }}{{ "," if not loop.last }}
    {%- endfor %}
  ) as font, cost

{% endmacro %}