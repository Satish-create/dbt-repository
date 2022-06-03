{% macro default_if_value_null(column_name, as_name=column_name) -%} 

  ifnull({{ column_name }},'{{var("dim_attribute_unknown_string")}}') as {{ as_name }}

{%- endmacro %}