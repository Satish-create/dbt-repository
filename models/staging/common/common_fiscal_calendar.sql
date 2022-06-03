WITH accounting_period AS (
    select * from {{ ref('netsuite_accounting_periods_source') }}
),
date_spine AS (
    select * from {{ ref('common_dates')}}
),
fiscal AS (
    select 
        ds.date_day                            AS date_key,
        ap_mon.accounting_period_name       AS fiscal_month,
        substring(ap_mon.accounting_period_name, 1, len(ap_mon.accounting_period_name) - 5) AS fiscal_month_name,
        case
            when ap_mon.accounting_period_name like '%Jan%' then cast(replace (substring(ap_mon.accounting_period_name, 1, len(ap_mon.accounting_period_name) - 5), 'Jan', '01') as int)
            when ap_mon.accounting_period_name like '%Feb%' then cast(replace (substring(ap_mon.accounting_period_name, 1, len(ap_mon.accounting_period_name) - 5), 'Feb', '02') as int)
            when ap_mon.accounting_period_name like '%Mar%' then cast(replace (substring(ap_mon.accounting_period_name, 1, len(ap_mon.accounting_period_name) - 5), 'Mar', '03') as int)
            when ap_mon.accounting_period_name like '%Apr%' then cast(replace (substring(ap_mon.accounting_period_name, 1, len(ap_mon.accounting_period_name) - 5), 'Apr', '04') as int)
            when ap_mon.accounting_period_name like '%May%' then cast(replace (substring(ap_mon.accounting_period_name, 1, len(ap_mon.accounting_period_name) - 5), 'May', '05') as int)
            when ap_mon.accounting_period_name like '%Jun%' then cast(replace (substring(ap_mon.accounting_period_name, 1, len(ap_mon.accounting_period_name) - 5), 'Jun', '06') as int)
            when ap_mon.accounting_period_name like '%Jul%' then cast(replace (substring(ap_mon.accounting_period_name, 1, len(ap_mon.accounting_period_name) - 5), 'Jul', '07') as int)
            when ap_mon.accounting_period_name like '%Aug%' then cast(replace (substring(ap_mon.accounting_period_name, 1, len(ap_mon.accounting_period_name) - 5), 'Aug', '08') as int)
            when ap_mon.accounting_period_name like '%Sep%' then cast(replace (substring(ap_mon.accounting_period_name, 1, len(ap_mon.accounting_period_name) - 5), 'Sep', '09') as int)
            when ap_mon.accounting_period_name like '%Oct%' then cast(replace (substring(ap_mon.accounting_period_name, 1, len(ap_mon.accounting_period_name) - 5), 'Oct', '10') as int)
            when ap_mon.accounting_period_name like '%Nov%' then cast(replace (substring(ap_mon.accounting_period_name, 1, len(ap_mon.accounting_period_name) - 5), 'Nov', '11') as int)
            when ap_mon.accounting_period_name like '%Dec%' then cast(replace (substring(ap_mon.accounting_period_name, 1, len(ap_mon.accounting_period_name) - 5), 'Dec', '12') as int)
        end                                 AS numeric_fiscal_month,
        ap_qtr.accounting_period_name       AS fiscal_quarter,
        cast(substring(ap_qtr.accounting_period_name, 2,2) as int)            AS numeric_fiscal_quarter,
        ap_year.accounting_period_name      AS fiscal_year,
        cast(substring(ap_year.accounting_period_name, 4, len(ap_year.accounting_period_name)) as int)
                                            AS numeric_fiscal_year,
        day_of_week_name                    AS day_of_the_week_name,
        day_of_week                         AS day_of_week,
        month_name                          AS month_name,
        month_name_short                    AS month_name_short,
        weeks.week_start_date,
        weeks.week_end_date,
        weeks.fiscal_week_of_year,
        weeks.fiscal_week_of_period
        -- week_of_year                        AS week_of_year
    from    date_spine ds 
    left outer join accounting_period ap_mon 
    on ds.date_day
    between ap_mon.accounting_period_start_date AND ap_mon.accounting_period_end_date
    and     ap_mon.is_year = 0 
    and     ap_mon.is_quarter = 0 
    and     ap_mon.is_adjustment_period = 0
    left outer join accounting_period ap_qtr
    on ds.date_day
    between ap_qtr.accounting_period_start_date AND ap_qtr.accounting_period_end_date
    and ap_qtr.is_quarter = 1
    and ap_qtr.is_year = 0
    and ap_qtr.is_adjustment_period = 0
    left outer join accounting_period ap_year
    on ds.date_day
    between ap_year.accounting_period_start_date AND ap_year.accounting_period_end_date
    and ap_year.is_year = 1
    and ap_year.is_quarter = 0
    and ap_year.is_adjustment_period = 0
    join {{ ref('common_fiscal_weeks')}} weeks 
    on ds.date_day = weeks.date_day
)

select * from fiscal
where numeric_fiscal_year = 2022
order by date_key
