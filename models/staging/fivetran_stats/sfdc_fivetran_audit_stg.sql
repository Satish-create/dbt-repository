with audit as (
    select
      Update_id,
      schema,
      "TABLE",
      "START",
      done,
      rows_updated_or_inserted
    from {{ ref('sfdc_fivetran_audit_source') }}
)


select * from audit