{% macro mask_data(column_name, type_val) -%} 

  {%- if ( var('mask_data')  == 'true') -%}
      mask_util.maskdata.replace_value({{ column_name }},'{{ type_val }}', '{{ var('mask_domain_name')}}' )     
  {%- else -%}
      {{ column_name }}
  {%- endif -%}

{%- endmacro %}
