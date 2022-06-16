--depends_on: {{ ref('fivetran_connector_source') }}

{%- call statement('my_statement', fetch_result=True) -%}
      select distinct connector_name from {{ ref('fivetran_connector_source') }} order by 1
{%- endcall -%}

{%- set connectors = load_result('my_statement')['data'] -%}


{% set connections=[] %}
{% for connector in  connectors %}
    
    {% if connector[0].upper() not in ['FIVETRAN_LOG_SCHEMA','FIVETRAN_NETSUITE'] %}
        {% do connections.append(connector[0].upper()) %}
    {% endif %}
{% endfor %}    


with final_audit as (

    {% for connection in connections %}
    
        SELECT

          Update_id,
          schema,
          "TABLE",
          "START",
          done,
          rows_updated_or_inserted,
          _FIVETRAN_SYNCED as sync_time
          
        from DA_TEAM_TEST_RAW.{{ connection }}.FIVETRAN_AUDIT
        --from {{ target.name }}_RAW.{{ connection }}.FIVETRAN_AUDIT

        {% if not loop.last %}
        UNION ALL
        {% endif %}
        
    
    {% endfor %}

)


select * from final_audit



