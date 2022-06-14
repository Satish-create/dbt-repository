with audit as (
    select 
      *
    from {{ source('netsuite_stats', 'fivetran_audit') }}
)


select * from audit