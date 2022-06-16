SELECT
  --date_trunc('year', usage_month) as year, 
  sum(credits_used) AS credits_used
FROM {{ref('snowflake_warehouse_metering_xf')}}
WHERE date_trunc('year', usage_month) = date_trunc('year', CURRENT_TIMESTAMP)::date
--group by 1