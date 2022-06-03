with finished_good as (
    select  * from {{ref('prep_finished_good')}}
),

final as (
    select  item_surrogate_key  as product_id,
            item_id             as product_netsuite_id,
            item_product_family as product_family,
            -- add product line
            --item_product_line   as product_line,
            item_code           as product_code,
            item_description    as product_description,
            item_sale_quantity  as product_sale_quantity,
            unit_type_name      as product_sale_unit_of_measure
    from    finished_good
)
select * from final