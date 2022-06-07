with final as (
    select 
      update_id,
      sum(rows_updated_or_inserted) as number_of_rows,
      DATEDIFF(second, min("START"), max(done)) as job_execution_time,
      count("TABLE") as total_number_of_tables
    from {{ ref('sfdc_fivetran_audit_stg') }}
group by 1

)


select * from final