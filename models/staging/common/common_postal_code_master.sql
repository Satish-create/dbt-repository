WITH territories AS (
    SELECT         *
    FROM {{ ref('gsheets_zipcodes_source')}}
),
postal_code_master AS (
    SELECT 
    /*
        zip_code        AS postal_code,
        state           AS state_code,
        city,
        county,
        'US'            as country_code --default to US
    */
        territory_code,
        territory_name,
        state_code,
        state_name,
        city,
        county,
        postal_code,
        'US'            as country_code --default to US
    from territories
)

SELECT * from postal_code_master