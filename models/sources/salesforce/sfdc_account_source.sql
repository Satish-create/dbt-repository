WITH account AS
(
    SELECT
        -- Primary key
        id                                  AS account_id,
        --account_number_c::string          AS account_number,
        account_number                      AS account_number,
        name::string                        AS account_name,       

        owner_id::string                    AS account_manager_id,
        parent_id::string                   AS account_parent_id,

        -- type_c::string                   AS account_type,
        type::string                        AS account_type,
        --sub_type_c::string                  AS account_subtype,
        --account_segment_c::string         AS account_segment,
        segment_c::string                   AS account_segment,
        industry::string                    AS account_industry,        
        -- record_type_id::string              AS account_record_type_id,

        billing_street::string              AS account_billing_street,
        billing_city::string                AS account_billing_city,
        billing_state::string               AS account_billing_state,
        billing_postal_code::string         AS account_billing_postal_code,

        --{{ mask_data('billing_postal_code','acc_bill_postal_code') }} as account_billing_postal_code,
        billing_country::string             AS account_billing_country,

        shipping_street::string             AS account_shipping_street,
        shipping_city::string               AS account_shipping_city,
        shipping_state::string              AS account_shipping_state,
        shipping_postal_code::string        AS account_shipping_postal_code,

        -- {{ mask_data('shipping_postal_code','acc_bill_postal_code') }} as account_shipping_postal_code,
        shipping_country::string            AS account_shipping_country,

        created_date::date                  AS account_created_date,
        last_modified_date::datetime        AS account_last_modified_date,
        is_deleted::boolean                 AS is_deleted,

        system_modstamp
        
    FROM {{ source('salesforce', 'account')}}
)

SELECT * FROM account --where account_id = '0015b00001ukTqHAAU'
