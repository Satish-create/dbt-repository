With dates as (
    select * from {{ ref('common_dates')}}
),
-- fiscal start end
fiscal_year_dates as  (
    select * from {{ ref('common_fiscal_year_dates')}}
),
exceptions as (
    select 
        date    AS date_day,
        fiscal_week,
        fiscal_year
    from {{ ref('seed_fiscal_dates_exceptions')}}
),
periods as (
    select
        date_day,
        fiscal_year,
        week_start_date,
        week_end_date,
        fiscal_week_of_year,
        fiscal_week_of_year-1           as week_num,
        mod(week_num::float, 13)        as w13_number,
        least(trunc(week_num/13),3)     as quarter_number,
        case 
            when fiscal_week_of_year = 53 then 3
            when w13_number between 0 and 3 then 1
            when w13_number between 4 and 7 then 2
            when w13_number between 8 and 12 then 3
        end                             as period_of_quarter,
        (quarter_number * 3) + period_of_quarter as period_number
        from fiscal_year_dates
        --where fiscal_year = 2022
        --order by date_day
)

select
    date_day,
    fiscal_year,
    week_start_date,
    week_end_date,
    fiscal_week_of_year,
    dense_rank() over (
        partition by fiscal_year, period_number
        order by fiscal_week_of_year)           as fiscal_week_of_period,
    period_number,
    quarter_number+1                            as fiscal_quarter,
    period_of_quarter                           as fiscal_period_of_quarter
from
    periods
-- where fiscal_year = 2022
order by 1,2

-- week_start_date
-- week_end_date
-- week_of_year

-- quarter_of_year
-- quarter_start_date
-- quarter_end_date