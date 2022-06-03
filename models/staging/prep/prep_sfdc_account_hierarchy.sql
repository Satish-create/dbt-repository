with clean_account as (
select *,
       case ifnull(account_parent_id,true) 
            when true then true 
            else false
       end as highest_parent_flag
from {{ref('prep_sfdc_account_clean')}}
),

account_hierarchy as (
select  
    connect_by_root account_id      as   parent_account_id,
    connect_by_root account_name    as   parent_account_name,
    account_id                      as   child_account_id,
    account_name                    as   child_account_name,
    is_customer_flag::boolean       as   is_customer_flag,
    level - 1                       as   depth_from_parent,
    sys_connect_by_path(account_name, ' -> ') as parent_child_path
from clean_account
connect by account_parent_id = prior account_id
),
account_hierarchy_with_parent_flag AS (
    select  h.*,
            highest_parent_flag::boolean as highest_parent_flag
    from    account_hierarchy h
    left outer join clean_account c
    on      h.parent_account_id = c.account_id
)

select * from account_hierarchy_with_parent_flag