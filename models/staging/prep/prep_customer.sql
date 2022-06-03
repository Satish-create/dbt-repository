    with customer as (
    select  *
    from    {{ref('prep_sfdc_account_clean')}}
    where   UPPER(account_record_type) = 'CUSTOMER'
),
netsuite_entity as (
    select * from {{ref('netsuite_entity_source')}}
),
final as (
    select  account_surrogate_key,
            account_id,  
            account_name,            
            account_number,          
            account_billing_city,    
            account_billing_state_code, 
            account_billing_postal_code, 
            account_billing_country_code, 
            account_type, 
            account_industry,   
            -- account_subtype, 
            account_segment,
            account_manager_name,
            --netsuite fields
            entity_id as netsuite_entity_id,
            entity_name as netsuite_customer_id,
            entity_full_name as netsuite_customer_name
from    customer c
left outer join netsuite_entity ne
on c.account_id = ne.entity_linked_salesforce_account_id
)

select * from final