WITH unit_type AS (
    SELECT  units_type_id   AS unit_type_id,
            name            AS unit_type_name
    FROM {{ source('netsuite', 'units_type')}}
)

SELECT * FROM unit_type