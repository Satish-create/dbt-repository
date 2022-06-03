{{ config( tags=["finance"]) }}

WITH transaction_lines AS 
(
    SELECT
        {{ dbt_utils.surrogate_key(['transaction_id', 'transaction_line_id']) }}
                                        AS transaction_line_unique_id,

        transaction_id::INTEGER         as transaction_id,
        transaction_line_id::INTEGER    as transaction_line_id,
        salesforce_order_line_id_io::VARCHAR as transaction_linked_salesforce_order_line_id,
        
        amount::FLOAT                   as transaction_amount,
        item_count::FLOAT               as transaction_quantity,
        unit_of_measure_id::FLOAT       as transaction_unit_of_measure_id,
        
        account_id::INTEGER             as transaction_account_id,
        item_id::INTEGER                as transaction_item_id,
        -- product_id::INTEGER             as transaction_product_id, --populated for Journal Entries
        department_id::INTEGER          as transaction_department_id
        -- class_id::INTEGER               as transaction_class_id, -- not used 
        --UPPER(non_posting_line)::VARCHAR  AS non_posting_line

        --tax_item_id,
        --quantity_packed                 AS transaction_quantity_packed,        
        --quantity_received_in_shipment   AS transaction_quantity_fulfilled
        --shipdate as transaction_shipdate,
        --item_unit_price,
        -- changed from quantityBilled
        --location_id

    FROM {{ source('netsuite', 'transaction_lines') }}

)

select * from transaction_lines