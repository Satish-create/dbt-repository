with salesforce_audit as (
    select
      Update_id,
      schema,
      "TABLE",
      "START",
      done,
      rows_updated_or_inserted,
      _FIVETRAN_SYNCED as sync_time
    from {{ ref('sfdc_fivetran_audit_source') }}
),

gsheets_audit as (
    select
      Update_id,
      schema,
      "TABLE",
      "START",
      done,
      rows_updated_or_inserted,
      _FIVETRAN_SYNCED
    from {{ ref('gsheets_fivetran_audit_source') }}
),

final_audit as (
  select * from salesforce_audit
  union all
  select * from gsheets_audit
)



select * from final_audit