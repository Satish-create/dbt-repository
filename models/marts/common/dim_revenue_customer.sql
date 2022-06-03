{{ config(materialized='view') }}
{{ config( tags=["finance","customer-master"]) }}

with customer as (
    select * from {{ref('dim_customer')}}
),
distinct_rev_customer as (
    select distinct customer_id from {{ref('fct_product_revenue_transaction')}}
),
final as(
    select  c.* 
    from    customer c    
    join    distinct_rev_customer r
    on      c.customer_id = r.customer_id
)
select * from final