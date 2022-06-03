{{ config( tags=["sales"]) }}

WITH territory_exception AS (
    SELECT  account_name        as sfdc_parent_account_id,
            territory           as territory_code,
            territory_nickname  as territory_name
    FROM {{ source('gsheets','exceptions')}}

)
SELECT * FROM territory_exception