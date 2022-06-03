with unique_territory as (

    select  distinct territory_code,
            territory_name
    from    {{ref('common_territory_geo_master')}}
    /*
    union   
    select  '{{var("dim_attribute_unknown_territory_code")}}',
            '{{var("dim_attribute_unknown_string")}}'
    union
    select  distinct territory_code,
            territory_name
    from    {{ref('gsheets_territory_exceptions_source')}}
    */
)
select  {{dbt_utils.surrogate_key(['UPPER(territory_code)'])}} as territory_id,
        territory_code,
        territory_name
from    unique_territory