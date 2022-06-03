{{ config( tags=["finance"]) }}

with date_master as 
(
     select * from {{ ref('common_date_master')}}
),

rev_transaction as 
(
     select * from {{ ref('fct_product_revenue_transaction')}}
),

final as
(
    select  DATE_KEY as revenue_transaction_date,
            DATE_NAME as TRANSACTION_DATE_NAME,
            IS_CURRENT_YEAR as TRANSACTION_IS_CURRENT_YEAR,
            IS_CURRENT_MONTH as TRANSACTION_IS_CURRENT_MONTH,
            IS_CURRENT_DAY as TRANSACTION_IS_CURRENT_DAY,
            IS_YEAR_TO_DATE as TRANSACTION_IS_YEAR_TO_DATE,
            IS_QTR_TO_DATE as TRANSACTION_IS_QTR_TO_DATE,
            YEAR as TRANSACTION_YEAR,
            QTR_NUM as TRANSACTION_QTR_NUM,
            QTR_NAME as TRANSACTION_QTR_NAME,
            MONTH_NUM as TRANSACTION_MONTH_NUM,
            MONTH_NAME as TRANSACTION_MONTH_NAME,
            WEEK_NUM as TRANSACTION_WEEK_NUM,
            WEEK_NAME as TRANSACTION_WEEK_NAME,
            WEEK_RANGE as TRANSACTION_WEEK_RANGE,
            WEEK_OF_QTR as TRANSACTION_WEEK_OF_QTR,
            DAY_OF_MONTH as TRANSACTION_DAY_OF_MONTH,
            DAY_OF_YEAR as TRANSACTION_DAY_OF_YEAR,
            DAY_NAME as TRANSACTION_DAY_NAME,
            DAY_WITH_MONTH_NAME as TRANSACTION_DAY_WITH_MONTH_NAME,
            DAY_OF_WEEK as TRANSACTION_DAY_OF_WEEK,
            PRIOR_DATE as TRANSACTION_PRIOR_DATE
    from    date_master
    where   year(date_key) in (select distinct year(revenue_transaction_date) from rev_transaction) 
)

select * from final order by revenue_transaction_date

