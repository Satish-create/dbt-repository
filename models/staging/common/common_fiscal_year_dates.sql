with year_month_end as (
    select 
        d.year_number as fiscal_year_number,
        d.month_end_date
    from {{ ref('common_dates')}} d
    where
        d.month_of_year = 12
    group by 1,2
),
weeks as (
    select 
        d.year_number,
        d.date_day as week_start_date,
        dateadd('d', 6, d.date_day) as week_end_date
    from
        {{ ref('common_dates')}} d 
    where
        date_part('dow', d.date_day) = 0
),
year_week_ends as (
    select 
        d.year_number as fiscal_year_number,
        d.week_end_date
    from
        weeks d 
    where
        date_part('month', d.week_start_date) = 12
    group by 1,2
),
weeks_at_month_end as (
    select 
        d.fiscal_year_number,
        d.week_end_date,
        m.month_end_date,
        rank() over
            (partition by d.fiscal_year_number
                order by
                abs(datediff('d', d.week_end_date, m.month_end_date))
            ) as closest_to_month_end
    from 
        year_week_ends d 
    join year_month_end m on d.fiscal_year_number = m.fiscal_year_number
),
fiscal_year_range as (
    select 
        fiscal_year,
        accounting_period_start_date    AS fiscal_year_start_date,
        accounting_period_end_date      AS fiscal_year_end_date
    from {{ ref('netsuite_accounting_periods_source')}}
    where is_year = 1
    /*
    select
        fiscal_year_number,
        dateadd('day', 1, lag(week_end_date) over(order by week_end_date)) as fiscal_year_start_date,
        week_end_date as fiscal_year_end_date
    from
        weeks_at_month_end
    where closest_to_month_end = 1 
    */
)
select
    d.date_day,
    m.fiscal_year,
    m.fiscal_year_start_date,
    m.fiscal_year_end_date,
    w.week_start_date,
    w.week_end_date,
    dense_rank()
        over(
            partition by m.fiscal_year
            order by w.week_start_date
        ) as fiscal_week_of_year
from
    {{ ref('common_dates')}} d 
    join
    fiscal_year_range m on d.date_day between m.fiscal_year_start_date and m.fiscal_year_end_date
    join
    weeks w on d.date_day between w.week_start_date and w.week_end_date
    where fiscal_year = 2022 order by date_day