{% set sql_statement %}
    select connetor_name from {{ ref('fivetran_conector_source') }}
{% endset %}

{%- set connectors = dbt_utils.get_query_results_as_dict(sql_statement) -%}

{{ connectors }}
