with final as (
    select 
      update_id,
      schema as connector_name,
      sum(rows_updated_or_inserted) as number_of_rows,
      DATEDIFF(second, min("START"), max(done)) as job_execution_time_sec,
      count("TABLE") as total_number_of_tables,
      max(sync_time) as last_Updated
    from {{ ref('fivetran_audit_stg') }}
group by 1,2

)


select * from final