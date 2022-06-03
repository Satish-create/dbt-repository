{{ config( tags=["finance","fact"]) }}

with product_rev_transactions as (
    select * 
    from    {{ref('prep_transaction_lines')}}
    where   is_product_revenue_transaction = 1
    --and     is_posting_transaction = 1
),
customer as (
    select * from {{ref('prep_customer')}}
),
product as (
    select * from {{ref('dim_product')}}
),
final as (
    select  prt.transaction_line_unique_id  as revenue_transaction_id,            
            prt.transaction_number          as revenue_transaction_number,
            prt.transaction_type            as revenue_transaction_type,
            prt.account_name                as revenue_gl_account_name,

            c.account_surrogate_key         as customer_id, --revenue_customer_id
            c.account_surrogate_key         as territory_customer_id, --dupe for BI for territory/customer mapping
            p.product_id                    as product_id,
            
            prt.transaction_amount * -1     as revenue_amount,
            prt.transaction_quantity * -1   as revenue_transaction_quantity,
            prt.transaction_unit_of_measure_id as revenue_transaction_unit_of_measure_id,

            case
                when p.product_id is null then revenue_transaction_quantity * 1
                else revenue_transaction_quantity * p.product_sale_quantity
            end                             as revenue_transaction_units,

            prt.accounting_period_end_date  as revenue_accounting_period_date, --accounting_period_date,
            prt.transaction_date            as revenue_transaction_date            
    from    product_rev_transactions prt
    left outer join customer c
    on      prt.transaction_entity_id = c.netsuite_entity_id
    left outer join product p
    on      prt.transaction_item_id = p.product_netsuite_id
)

select * from final