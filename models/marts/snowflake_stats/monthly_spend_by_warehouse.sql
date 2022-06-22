SELECT  usage_month,
        warehouse_name,
        sum(credits_used) as credits,
        sum(dollars_spent) as spend
FROM {{ref('snowflake_warehouse_metering_xf')}}
WHERE usage_month <= date_trunc('month', CURRENT_TIMESTAMP)::DATE
  --AND usage_month <= date_trunc('month', CURRENT_TIMESTAMP)::date
  --AND warehouse_name='COMPUTE_WH'
GROUP BY 1, 2
ORDER BY 1 DESC