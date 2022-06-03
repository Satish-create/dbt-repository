with payments as (
    select
         id as payment_id,
         orderid as order_id,
         paymentmethod as payment_method,
         status,
         {{cents_to_dollar('amount')}} as amount,
         created as created_date
    /*from raw.stripe.payment*/
    from {{ source('stripe','payment')}}
)


select * from payments