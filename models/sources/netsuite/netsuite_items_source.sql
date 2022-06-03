WITH items AS
(
    SELECT 
    item_id::INTEGER            AS item_id,
    --full_name                 AS item_display_name,

    {{ mask_data('name','item_name') }} as item_name,     
    {{ mask_data('salesdescription','item_salesdescription') }} as item_sales_description, 
    {{ mask_data('purchasedescription','item_salesdescription') }} as item_purchase_description, 

    --name::varchar(256)          AS item_name,
    --salesdescription            AS item_sales_description,
    --purchasedescription         AS item_purchase_description,
    product_family_id           AS product_family_id,
    --item_group_id,    
    CASE upper(finished_good)       WHEN 'T'    THEN 1 ELSE 0 END AS item_is_finished_good_flag,
    CASE upper(isinactive)          WHEN 'NO'   THEN 0 ELSE 1 END AS item_is_inactive_flag,
    CASE upper(lot_numbered_item)   WHEN 'T'    THEN 1 ELSE 0 END AS item_is_lot_numbered_flag,
    salesforce_id_io            AS item_linked_salesforce_id,
    sale_quantity::FLOAT        AS item_sale_quantity,
    sale_unit_id                AS item_sale_unit_id,
    --scrap_account_id,
    type_name                   AS item_type,
    --units_type_id,
    created                     AS item_date_created,
    date_last_modified          AS item_date_last_modified
    --income_account_id,    
    --asset_account_id,
    --class_id,
    --commodity_id,
    --department_id,
    --expense_account_id,
    --cost_category,
    --created as item_CreatedDate,
    --current_rev_id,    
    --_fivetran_synced                    AS synced_from_source_date,
FROM {{ source('netsuite', 'items') }}
)

SELECT * FROM items --where item_is_finished_good_flag = 1
