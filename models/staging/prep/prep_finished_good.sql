with item as (
    select * from {{ref('netsuite_items_source')}}
),
product_family as (
    select * from {{ref('netsuite_product_family_source')}}
),
unit_type as (
    select * from {{ref('netsuite_units_type_source')}}
),
final as (
select  {{dbt_utils.surrogate_key(['i.item_id'])}} as item_surrogate_key,
        item_id,
        item_name as item_code,
        ifnull(pf.product_family_name, 'Accessories') as item_product_family,
        --ifnull(pl.product_line, 'Unknown') as item_product_line,
        item_sales_description as item_description,
        ifnull(item_sale_quantity,1) as item_sale_quantity,
        ifnull(ut.unit_type_name, 'Each') as unit_type_name
from    item i
left outer join    product_family pf
on      i.product_family_id = pf.product_family_id
-- left outer join    product_line pl
-- on      UPPER(i.item_name) = pl.product_code
left outer join    unit_type ut
on      i.item_sale_unit_id = ut.unit_type_id
where i.item_is_finished_good_flag = 1 )

select * from final