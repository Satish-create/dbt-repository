{{ config( tags=["finance"]) }}

WITH accounts AS (
    SELECT  account_id,
            parent_id               AS account_parent_id,
            type_name               AS account_type,
            name                    AS account_name,    
            full_name               AS account_full_name,
            accountnumber::string   AS account_number,    
            openbalance             AS open_balance,
            is_leftside             AS is_leftside_account
    FROM {{ source('netsuite', 'accounts')}}
)

SELECT * FROM accounts