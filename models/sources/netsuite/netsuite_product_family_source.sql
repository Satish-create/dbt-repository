WITH product_family AS (
    SELECT  list_id         AS product_family_id,
            --list_item_name  AS product_family_name
            {{ mask_data('list_item_name','pf_list_item_name') }} as product_family_name
    FROM {{ source('netsuite', 'product_family')}}
)

SELECT * FROM product_family