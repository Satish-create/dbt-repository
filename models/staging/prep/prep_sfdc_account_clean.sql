WITH account AS (
    select * from {{ref('sfdc_account_source')}}
),
record_type AS 
(
    select * from {{ref('sfdc_record_type_source')}}
),
account_manager AS 
(
    select * from {{ref('sfdc_user_source')}}
),
state_map as (
    select distinct STATE_CODE,STATE_FULL_NAME,COUNTRY_CODE
    from   {{ref('seed_country_state_names_list')}}
),
country_map as 
(
    select  distinct lower(input_country_value) as input_country_value, 
                     upper(clean_country_code) as clean_country_code
    from    {{ref('seed_country_cleanup_list')}}
),
postal_code_master AS 
(
    select * from {{ref('common_postal_code_master')}}
),
account_clean_country AS 
(
    select  a.account_id,            
            {{ default_if_country_null('cb.clean_country_code','account_billing_country_code') }},
            {{ default_if_country_null('cs.clean_country_code','account_shipping_country_code') }},
            case (account_billing_country) 
                when 'PUERTO RICO' then 'PR'
                else account_billing_state
            end as account_billing_state,
            account_billing_postal_code,
            case (account_shipping_country)
                when 'PUERTO RICO' then 'PR'
                else account_shipping_state
            end as account_shipping_state,
            account_shipping_postal_code
    from    account a
    left outer join country_map cb
    on lower(a.account_billing_country) = cb.input_country_value
    left outer join country_map cs
    on lower(a.account_shipping_country) = cs.input_country_value
),
account_clean_country_state AS
(
    select  a.account_id,
            --Billing
            {{ default_if_value_null('coalesce(stb.state_code,pm_bill.state_code)','account_billing_state_code') }},
            account_billing_country_code,
            case account_billing_country_code
                when 'US' then left(account_billing_postal_code,5)
                else  account_billing_postal_code
            end as account_billing_postal_code,

            --Shipping
            {{ default_if_value_null('coalesce(sts.state_code,pm_ship.state_code)','account_shipping_state_code') }},
            account_shipping_country_code,
            case account_shipping_country_code
                when 'US' then left(account_shipping_postal_code,5)
                else  account_shipping_postal_code
            end as account_shipping_postal_code
    from    account_clean_country a
    
    left outer join state_map stb
    on  lower(a.account_billing_country_code) = lower(stb.country_code)
    and lower(a.account_billing_state) = lower(stb.state_full_name)
    
    left outer join state_map sts
    on  lower(a.account_shipping_country_code) = lower(sts.country_code)
    and lower(a.account_shipping_state) = lower(sts.state_full_name)
    
    left outer join postal_code_master pm_bill
    on  a.account_billing_postal_code = pm_bill.postal_code
    and a.account_billing_country_code = pm_bill.country_code
    
    left outer join postal_code_master pm_ship
    on  a.account_shipping_postal_code = pm_ship.postal_code
    and a.account_shipping_country_code = pm_ship.country_code
),
clean_account as (
select {{dbt_utils.surrogate_key(['a.account_id'])}} as account_surrogate_key, 
        a.account_id,
        {{ default_if_value_null('a.account_name','account_name') }},
        {{ default_if_value_null('a.account_number','account_number') }},
        --Billing Address
        {{ default_if_value_null('a.account_billing_street','account_billing_street') }},
        {{ default_if_value_null('a.account_billing_city','account_billing_city') }},
        aclean.account_billing_state_code,
        {{ default_if_value_null('aclean.account_billing_postal_code','account_billing_postal_code') }},
        aclean.account_billing_country_code,
        --Shipping Address
        {{ default_if_value_null('a.account_shipping_street','account_shipping_street') }},
        {{ default_if_value_null('a.account_shipping_city','account_shipping_city') }},        
        aclean.account_shipping_state_code,
        {{ default_if_value_null('aclean.account_shipping_postal_code','account_shipping_postal_code') }},
        aclean.account_shipping_country_code,
        {{ default_if_value_null('a.account_type','account_type') }},
        -- {{ default_if_value_null('a.account_subtype','account_subtype') }},
        {{ default_if_value_null('a.account_industry','account_industry') }},
        --KEY FIELDS
        UPPER(a.account_type) as account_record_type,
        case account_record_type
            when 'CUSTOMER' then true
            else false
        end as is_customer_flag,
        -- UPPER(rt.RECORD_TYPE_NAME) as account_record_type, --CUSTOMER, SHIP TO
        --case account_record_type
        --  when 'CUSTOMER' then true
        --    else false
        --end as is_customer_flag,
        a.account_parent_id,
        {{ default_if_value_null('a.account_segment','account_segment') }} ,
        --MISC FIELDS
        a.account_manager_id,
        am.user_full_name as account_manager_name,
        --DATES
        a.account_created_date,
        a.account_last_modified_date
from    account a
join    account_clean_country_state aclean
on      a.account_id = aclean.account_id
--join    record_type rt --exclude accounts without a record type
--on      a.account_record_type_id = rt.record_type_id
left outer join account_manager am
on      a.account_manager_id = am.user_id
where   a.is_deleted = false )

select  * from clean_account --where account_id = '0013t00001hi8hKAAQ'

