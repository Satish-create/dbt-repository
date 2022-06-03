WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'user') }}

), renamed AS(

    SELECT
      -- ids
      id                     AS user_id,
      -- username            AS user_name,
      name                   AS user_full_name,
      -- last_name           AS user_last_name,
      -- first_name          AS user_first_name,
      -- email               AS user_email,
      -- company_name        AS company_name,
      
      -- info
      --title               AS title,
      --department          AS department,
      is_active           AS is_active,
      
      --metadata
      created_by_id       AS created_by_id,
      created_date        AS created_date,
      last_modified_by_id AS last_modified_id,
      last_modified_date  AS last_modified_date,
      system_modstamp

    FROM source

)

SELECT *
FROM renamed
