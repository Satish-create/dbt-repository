{% set dim_attribute_unknown_territory_id = 'aa363f5f96c3643f9df771f374849e2b' %}

with cust_terr_exception as (
    select * from {{ref('prep_territory_account_exception')}}
),
terr_geo_master as (
    select * from {{ref('common_territory_geo_master')}}
),
dim_customer as (
    select * from {{ref('dim_customer')}}
),
dim_territory as (
    select * from {{ref('dim_territory')}}
),
final as (
    select  ifnull(dt.territory_id,'{{dim_attribute_unknown_territory_id}}') as territory_id,
            dc.customer_id as customer_id
            --customer_crm_id,
            --customer_name,
            --tm.territory_code
    from    dim_customer dc
    left outer join terr_geo_master tm
    on      dc.customer_billing_postal_code = tm.postal_code    
    left outer join dim_territory dt
    on UPPER(dt.territory_code) = UPPER(tm.territory_code)
    where   customer_crm_id not in (select account_id from cust_terr_exception)
    union  
    select     ifnull(dt.territory_id,'{{dim_attribute_unknown_territory_id}}') as territory_id,
               dc.customer_id
    --         account_id,
    --         account_name,
    --         territory_code
    from    cust_terr_exception ct
    join    dim_customer dc
    on      ct.account_id = dc.customer_crm_id
    left outer join dim_territory dt
    on UPPER(ct.territory_code) = UPPER(dt.territory_code)
)

select  territory_id as territory_id,
        customer_id  as territory_customer_id
from final