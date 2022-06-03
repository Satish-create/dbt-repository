with transactions as 
(
     select * from {{ ref('netsuite_transactions_source')}}
),

transaction_lines as 
(
     select * from {{ ref('netsuite_transaction_lines_source')}}
),

item as (
    select * from {{ ref('netsuite_items_source')}}
),

accounting_periods as (
    select * from {{ ref('netsuite_accounting_periods_source')}}
),

gl_accounts as (
    select * from {{ ref('prep_general_ledger_account')}}
),

entity as (
    select * from {{ ref('netsuite_entity_source')}}
),

final as
(
    select
        tl.transaction_line_unique_id,
        t.transaction_id,
        t.transaction_number,
        t.transaction_type,
        --tl.transaction_line_id,

        -- case ifnull(tl.non_posting_line,'YES')
        --     when 'YES' then 0
        --     else 1
        -- end as is_posting_transaction,

        
        t.transaction_entity_id,
        e.entity_full_name,        

        t.transaction_accounting_period_id,
        ap.accounting_period_start_date,
        ap.accounting_period_end_date,
        t.transaction_date,

        tl.transaction_account_id,
        gla.account_number,
        gla.account_name,
        gla.account_type,
        gla.is_product_revenue_account as is_product_revenue_transaction,

        tl.transaction_item_id,        
        i.item_name,
        i.item_sales_description,

        --tl.transaction_product_id --TO DO - need to model for JE transactions,
        
        tl.transaction_quantity,
        tl.transaction_unit_of_measure_id,
        tl.transaction_amount        
    from transactions t
    left outer join transaction_lines tl 
        on t.transaction_id = tl.transaction_id
    left outer join item i
        on tl.transaction_item_id = i.item_id
    left outer join entity e
        on t.transaction_entity_id = e.entity_id
    left outer join accounting_periods ap
         on ap.accounting_period_id = t.transaction_accounting_period_id
    left outer join gl_accounts gla
        on tl.transaction_account_id = gla.account_id
)

select * from final
