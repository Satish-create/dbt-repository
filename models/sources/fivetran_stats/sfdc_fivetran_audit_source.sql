with audit as (
    select 
      *
    from {{ source('salesforce_stats', 'fivetran_audit') }}
)


select * from audit