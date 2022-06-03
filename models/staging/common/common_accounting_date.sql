WITH accounting_period AS (
    select * from {{ ref('netsuite_accounting_periods_source') }}
),
date_spine AS (
    select 
        date_day,
        day_of_week,
        day_of_week_name,
        day_of_week_name_short,
        day_of_month,
        day_of_year,
        week_of_year,
        month_name,
        month_name_short,
        year_number
    from {{ ref('common_dates')}}
/*
  {{ dbt_utils.date_spine(
      start_date="to_date('12/01/2018', 'mm/dd/yyyy')",
      datepart="day",
      end_date="to_date('01/01/2024','mm/dd/yyyy')"
     )
  }}
*/
),
calculated as (
    select  date_day as date_key,
            ap_month.accounting_period_name as fiscal_month,
            ap_qtr.accounting_period_name   as fiscal_quarter,
            ap_year.accounting_period_name  as fiscal_year
    from    date_spine ds
    left outer join    accounting_period ap_month
    on      ds.date_day >= ap_month.accounting_period_start_date 
    and     ds.date_day <= ap_month.accounting_period_end_date
    and     ap_month.is_year = 0 
    and     ap_month.is_quarter = 0 
    and     ap_month.is_adjustment_period = 0
    left outer join    accounting_period ap_qtr
    on      ds.date_day >= ap_qtr.accounting_period_start_date 
    and     ds.date_day <= ap_qtr.accounting_period_end_date
    and     ap_qtr.is_year = 0 
    and     ap_qtr.is_quarter = 1 
    and     ap_qtr.is_adjustment_period = 0
    left outer join    accounting_period ap_year
    on      ds.date_day >= ap_year.accounting_period_start_date 
    and     ds.date_day <= ap_year.accounting_period_end_date
    and     ap_year.is_year = 1 
    and     ap_year.is_quarter = 0 
    and     ap_year.is_adjustment_period = 0
)
select * from calculated order by date_key
--where year(date_key) = 2021 order by date_key


