SELECT sum(credits_used) AS credits_used
FROM  {{ref('snowflake_warehouse_metering_xf')}}
WHERE  usage_month = date_trunc('month', CURRENT_TIMESTAMP)::date