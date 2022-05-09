with payments as (
    select
        order_id,
        count(*) as number_orders,
        sum(coalesce(amount,0)) as total_revenvue
    from {{ ref('stg_payments') }}
    where status='success'
    group by order_id    
)


select * from payments