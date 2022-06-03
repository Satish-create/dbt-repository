WITH entity AS 
(
    SELECT 
        -- Primary Key
       entity_id,
       name             AS entity_name,       
       full_name        AS entity_full_name,       
       salesforce_id_io AS entity_linked_salesforce_account_id
    --    entity_type,
    --    city,
    --    state,
    --    zipcode,
    --    country,
    --    create_date,
    --    date_last_modified AS entity_last_modified

from {{ source('netsuite', 'entity') }}

)
select * from entity