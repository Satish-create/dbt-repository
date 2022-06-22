with cte as (
    select 
        
        warehouse_id,
        warehouse_name,
        start_time,
        end_time,
        usage_day,
        credits_used,
        dollars_spent
        
    from {{ ref('snowflake_warehouse_metering_xf') }}
)


select *
from cte