{% macro mask_column(column_name, as_name=column_name) -%} 

  {%- if ( var('mask_column')  == 'true') -%}
      MD5({{ column_name }}) as {{ as_name }}
  {%- else -%}
      {{ column_name }} as {{ as_name }}
  {%- endif -%}

{%- endmacro %}