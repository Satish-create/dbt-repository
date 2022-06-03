{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set default_database = target.database -%}
    {%- if custom_database_name is none -%}

        {{target.name}}_Analytics

    {%- else -%}
        {{target.name}}_{{custom_database_name}}
    {%- endif -%}

{%- endmacro %} 