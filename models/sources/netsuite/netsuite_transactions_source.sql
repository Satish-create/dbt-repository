{{ config( tags=["finance"]) }}

WITH transactions AS
(
   SELECT
      transaction_id          AS transaction_id,
      tranid                  AS transaction_number,
      entity_id               as transaction_entity_id,
      accounting_period_id    as transaction_accounting_period_id,
      salesforce_order_idio   as transaction_linked_salesforce_order_id,
      
      transaction_type        as transaction_type,
      memo                    as transaction_memo,
      status                  as transaction_status,      
      
      trandate::date          AS transaction_date
      
      --created_from_id,
      --create_date,
      --due_date,
      --discount_amount,
      --shipaddress             AS shipping_address,
      --customer_shipping_method,
      --reason_for_return_id,
      --location_id,      
      --so_reason_code_id,
      --currency_id
      --created_by_id
   FROM {{ source('netsuite', 'transactions') }}

)

SELECT * FROM transactions
