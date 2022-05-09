{% macro cents_to_dollar(column_name) %}
round(1.0*{{column_name}}/100,2)
{% endmacro %}