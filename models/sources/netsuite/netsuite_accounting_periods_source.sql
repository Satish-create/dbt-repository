{{ config( tags=["finance"]) }}
WITH renamed AS (
    SELECT 
    -- Primary key
    accounting_period_id    AS accounting_period_id,

    name::varchar           AS accounting_period_name,
    starting::date          AS accounting_period_start_date,
    ending::date            AS accounting_period_end_date,
    year(ending)            AS fiscal_year,
    case upper(ifnull(quarter,'NO'))            
        when 'YES' then 1
        else 0
    end AS is_quarter,
    case upper(ifnull(year_0,'NO'))
        when 'YES' then 1
        else 0
    end AS is_year,
    case upper(ifnull(is_adjustment,'NO'))
        when 'YES' then 1
        else 0
    end AS is_adjustment_period
    /*
    -- Foreign Keys
    parent_id,
    year_id,

    -- Info
    name                        AS accounting_period_name,
    full_name                   AS accounting_period_full_name,
    fiscal_calendar_id,
    closed_on                   AS accounting_period_close_date,
    ending                      AS accounting_period_end_date,

    -- Meta
    locked_accounts_payable     AS is_accounts_payable_locked,
    locked_accounts_receivable  AS is_acounts_receivables_locked,
    locked_all                  AS is_all_locked,
    locked_payroll              AS is_payroll_locked,
    closed                      AS is_accounting_period_closed,
    closed_accounts_payable     AS is_accounts_payable_closed,
    closed_accounts_receivable  AS is_accounts_receivables_closed,
    closed_all                  AS is_all_closed,
    closed_payroll              AS is_payroll_closed,
    isinactive                  AS is_accounting_period_inactive,
    
    quarter                     AS is_quarter,
    year_0                      AS is_year

    */
    FROM {{ source('netsuite', 'accounting_periods')}}
)

SELECT * FROM renamed