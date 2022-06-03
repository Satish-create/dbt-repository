WITH record_type AS
(
  SELECT
      id::string    AS record_type_id,
      name::string  AS record_type_name

      -- developer_name       AS record_type_developer_name,
      -- namespace_prefix     AS record_type_namespace_prefix,
      -- description:string   AS record_type_description,
      -- business_process_id  AS record_type_business_preocess_id,
      -- sobject_type         AS record_type_object_type,
      -- is_active            AS is_active,
      -- system_modstamp
  FROM {{ source('salesforce','record_type') }}
)
SELECT * FROM record_type
