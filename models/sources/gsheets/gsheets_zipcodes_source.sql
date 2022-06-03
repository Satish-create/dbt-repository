{{ config( tags=["sales"]) }}

WITH territories AS (
    SELECT 
        territory_code       AS territory_code,
        territory            AS territory_name,
        state                AS state_code,
        statefullname        AS state_name,
        county               AS county,
        city                 AS city,
        zipcode             AS postal_code
    FROM {{ source('gsheets','zipcodes')}}

)

SELECT * FROM territories