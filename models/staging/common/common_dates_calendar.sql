WITH date_spine AS (
    SELECT * FROM {{ ref('common_dates')}}
),
historical AS (
    SELECT
        date_day                                AS date_key,
        prior_date_day                          AS prior_date_day,
        prior_year_date_day                     AS prior_year_date_day,
        day_of_week                             AS day_of_week,
        day_of_week_name                        AS day_of_week_name,
        day_of_week_name_short                  AS day_of_week_name_short,
        day_of_month                            AS day_of_month,
        ROW_NUMBER() OVER (PARTITION BY year_number, quarter_of_year ORDER BY date_key) AS day_of_quarter,
        day_of_year                             AS day_of_year,
        week_start_date                         AS week_start_date,
        week_end_date                           AS week_end_date,
        12-floor((DATEDIFF(day, date_key, quarter_end_date)/7))    AS week_of_quarter,
        week_of_year                            AS week_of_year,
        month_of_year                           AS month_of_year,
        month_name                              AS month_name,
        month_name_short                        AS month_name_short,
        quarter_of_year                         AS quarter_of_year,
        quarter_start_date                      AS quarter_start_date,
        quarter_end_date                        AS quarter_end_date,
        year_number                             AS year_number,
        year_start_date                         AS year_start_date,
        year_end_date                           AS year_end_date 
    FROM date_spine
)

SELECT * FROM historical