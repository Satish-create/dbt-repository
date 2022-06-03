with customer as (
    select  *
    from    {{ref('prep_customer')}}
),
final as (
select  account_surrogate_key   as customer_id,
        account_id              as customer_crm_id,
        account_name            as customer_name,
        account_number          as customer_account_number,
        account_billing_city    as customer_billing_city,
        account_billing_state_code as customer_billing_state_code,
        account_billing_postal_code as customer_billing_postal_code,
        account_billing_country_code as customer_billing_country_code,
        account_type    as customer_type,
        --account_subtype as customer_subtype
        account_industry as customer_industry,
        account_segment as customer_segment,
        account_manager_name as customer_account_manager
from    customer c
)

select * from final
--where customer_id = '0015b00001srxjEAAQ'