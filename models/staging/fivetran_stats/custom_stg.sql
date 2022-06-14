{%- call statement('my_statement', fetch_result=True) -%}
      select distinct connector_name from {{ source('fivetran_connector', 'CONNECTOR') }}
{%- endcall -%}

{%- set connectors = load_result('my_statement')['data'] -%}

--{{ connectors }}


final_audit as (

    {% for connector in connectors %}
    {{connector[0]}}

        SELECT

          Update_id,
          schema,
          "TABLE",
          "START",
          done,
          rows_updated_or_inserted,
          _FIVETRAN_SYNCED as sync_time
          
        from DA_TEAM_TEST_RAW.{{ connector[0] }}.fivetran_audit

        {% if not loop.last %}
        UNION ALL
        {% endif %}
        
    
    {% endfor %}

)


select * from final_audit



