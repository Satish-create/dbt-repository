with territory_master as (
SELECT
        territory_code,
        territory_name,
        state_code,
        state_name,
        city,
        county,
        postal_code,
        'US'            as country_code --default to US
FROM {{ ref('gsheets_zipcodes_source')}}
)
select * from territory_master