with date_gen as
(

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2018-01-01' as date)",
        end_date="cast('2023-12-31' as date)"
        )
    }}
),

date_spine as (
    select date_day as DATE_KEY from date_gen
),

stg_date_weekrange as 
(
    select
     DATE_KEY,
        ceil(DAYOFYEAR(DATE_KEY)/7)         as WeekNum_StartofYear_Date,
        year(DATE_KEY)                      as Year_Date,
        --YEAROFWEEKISO(DATE_KEY),
        DAY(DATE_KEY)                       as DayNumMonth_Date,
        DAYOFYEAR(DATE_KEY)                 as DayNumYear_Date,
        monthname(date_key)                 as month_name,
        CAST(MONTH(DATE_KEY) as STRING) || '-' || monthname(date_key)               as month_with_num,
        min(DayNumYear_Date) OVER (PARTITION BY year_date, WeekNum_StartofYear_Date) as wstart,
        max(DayNumYear_Date) OVER (PARTITION BY year_date, WeekNum_StartofYear_Date) as wend,
        --nameofmonth OVER (PARTITION BY weekday),
        dayname(DATE_KEY) as DayName_Date,
        monthname(date_key) || ' ' || cast(DAYOFMONTH(DATE_KEY) as string) as DayMonthName_Date,
        monthname(date_key) || ' ' || cast(DAYOFMONTH(DATE_KEY) as string) || ', ' || cast(year(DATE_KEY) as string)  as DayMonthYearName_Date,
        --DAYOFWEEK(DATE_KEY),  --Sun=0, Mon=1
        DAYOFWEEKISO(DATE_KEY) as DayofWeekNum_Date, --Mon=1,Sun=7
        --WEEK(DATE_KEY) as WeekNum_Date,
        'WK-' || CAST(WeekNum_StartofYear_Date as string) as WeekNumName_Date,
        ceil((DATEDIFF('day',DATE_TRUNC('QUARTER',date_key), date_key)+1)/7) as WeekNumOfQtr,
        'WK-' || CAST(WeekNumOfQtr as string) as WeekNumNameOfQtr,
        --WEEKOFYEAR(DATE_KEY),
        --WEEKISO(DATE_KEY),
        MONTH(DATE_KEY) as MonthNum_Date,      
        QUARTER(DATE_KEY) as QtrNum_Date,
        'Q' || CAST(QUARTER(DATE_KEY) as string) as QtrNumName_Date
    from date_spine
),

range as
(
select distinct wstart,wend,year_date from stg_date_weekrange
),

startslot as 
(
select  d.year_date,d.wstart,d.wend, month_name as mon1, daynummonth_date as day1
from    stg_date_weekrange d
join range cte
on d.daynumyear_date = cte.wstart and d.year_date = cte.year_date
),

endslot as 
(
select  d.year_date,d.wstart,d.wend, cte.mon1, cte.day1,month_name as mon2, daynummonth_date as day2
from    stg_date_weekrange d
join    startslot cte
on      d.daynumyear_date = cte.wend and d.year_date = cte.year_date
),

final as(
    select
        d.date_key,
        daymonthyearname_date as date_name,
        case datediff(year,d.date_key,current_date())
            when 0 then 1
            else 0
        end as is_current_year,
        case datediff(month,d.date_key,current_date())
            when 0 then 1
            else 0
        end as is_current_month,
        case datediff(day,d.date_key,current_date())
            when 0 then 1
            else 0
        end as is_current_day,
        case 
            when (datediff(year,d.date_key,current_date()) = 0 and datediff(day,d.date_key,current_date())>= 0) then 1
            else 0
        end as is_year_to_date,
        case 
            when (datediff(year,d.date_key,current_date()) = 0 and 
        datediff(day,d.date_key,current_date())>= 0  and
        QUARTER(DATE_KEY) = QUARTER(current_date())) then 1
            else 0
        end as is_qtr_to_date,
        d.year_date as year,
        qtrnum_date as qtr_num,
        qtrnumname_date as qtr_name,        
        monthnum_date as month_num,
        month_with_num as month_name,
        WeekNum_StartofYear_Date as week_num,
        WeekNumName_Date as week_name,
        mon1 || cast(day1 as string) || '-' || mon2 || cast(day2 as string) as week_range,
        WeekNumNameOfQtr as week_of_qtr,
        daynummonth_date as day_of_month,
        daynumyear_date as day_of_year,
        dayname_date as day_name,
        daymonthname_date as day_with_month_name,
        dayofweeknum_date as day_of_week,
        (d.date_key - INTERVAL '1 day') as prior_date
    from stg_date_weekrange d
    join endslot e
    on d.Year_Date = e.year_date
    and d.wstart = e.wstart
    and d.wend = e.wend

)

select * from final