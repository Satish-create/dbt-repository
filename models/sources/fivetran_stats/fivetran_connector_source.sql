with connector as (
    select * from {{ source('fivetran_connector', 'CONNECTOR') }}
)

select * from connector