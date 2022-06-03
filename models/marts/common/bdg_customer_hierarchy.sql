with sfdc_account_hierarchy as (
    select * from {{ref('prep_sfdc_account_hierarchy')}}
),
customer as (
    select * from {{ref('dim_customer')}}
),
customer_hierarchy as (
    select  parent.customer_id      as parent_customer_id, --customer_id --links to the DIM CUSTOMER table,
            parent.customer_name    as parent_customer_name,
            parent.customer_crm_id  as parent_customer_crm_id,
            child.customer_id       as child_customer_id, --revenue_customer_id --links to the FACT REVENUE table,
            child.customer_name     as child_customer_name,
            child.customer_crm_id   as child_customer_crm_id,
            case h.highest_parent_flag
                when true then 'Yes'
                else 'No'
            end as highest_parent_flag
    from    sfdc_account_hierarchy h
    join    customer parent
    on      h.parent_account_id = parent.customer_crm_id
    join    customer child
    on      h.child_account_id = child.customer_crm_id
)
select * from customer_hierarchy