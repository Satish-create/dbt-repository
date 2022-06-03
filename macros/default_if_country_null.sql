{% macro default_if_country_null(column_name, as_name=column_name) -%} 

  ifnull({{ column_name }},'{{var("dim_attribute_unknown_country")}}') as {{ as_name }}

{%- endmacro %}