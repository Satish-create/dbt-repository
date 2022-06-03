{{ config( tags=["finance"]) }}

with gl_accounts as (
    select * from {{ref('netsuite_accounts_source')}}
),

final as
(
    select
        account_id,
        account_parent_id,
        account_number,
        case left(account_number,4) 
            when '4000' then 1
            else 0
        end as is_product_revenue_account,
        account_name,
        --LEVEL as account_level,
        account_type,
        --connect_by_root account_id as top_level_account_id,
        --connect_by_root acc_fullname as top_level_account_name,
        --connect_by_root acc_type_name as top_level_account_type,
        --sys_connect_by_path(name, ' -> ') as account_list,
        case UPPER(is_leftside_account) when 'T' then 'DR' else 'CR' end as account_debit_or_credit,
        open_balance,
        case
            when LOWER(account_type) in ('accounts receivable', 'bank', 'deferred expense', 'fixed asset', 'other asset', 'other current asset', 'unbilled receivable')   then 'Asset'
            when LOWER(account_type) in ('cost of goods sold', 'expense', 'other expense') then 'Expense'
            when LOWER(account_type) in ('income', 'other income') then 'Revenue'
            when LOWER(account_type) in ('accounts payable', 'credit card', 'deferred revenue', 'long term liability', 'other current liability') then 'Liability'
            when LOWER(account_type) in ('equity', 'retained earnings', 'net income') then 'Equity'
            else account_type --if none of the above then retain type as the category value
      end as account_category
  from  gl_accounts
  --connect by parent_id = prior account_id
)

select * from final