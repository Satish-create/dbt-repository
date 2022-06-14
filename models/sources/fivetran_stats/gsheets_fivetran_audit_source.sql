with audit as (
    select 
      *
    from {{ source('gsheets_stats', 'fivetran_audit') }}
)


select * from audit