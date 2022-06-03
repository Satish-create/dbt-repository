with terr_exception as (
    select * from {{ref('gsheets_territory_exceptions_source')}}
),
account_hierarchy as (
    select * from {{ref('prep_sfdc_account_hierarchy')}}
),
territory_account_exception as (
    select  te.territory_code,
            ah.child_account_id as account_id,
            ah.child_account_name as account_name
    from    terr_exception te
    join    account_hierarchy ah
    on      te.sfdc_parent_account_id = ah.parent_account_id
    where   ah.is_customer_flag = true
)
select * from territory_account_exception