WITH 
cte_source AS (
    SELECT *
    FROM {{ source('sixsense', 'raw_intent') }}
),
cte_transformed AS (
    SELECT 
        "CRM Account ID" AS crm_account_id
        ,"CRM Account Name" AS crm_account_name
        ,"CRM Account Country" AS crm_account_country
        ,"CRM Account Domain" AS crm_account_domain
        ,"Keyword Researched" AS keyword_researched
        ,"Keyword Category" AS keyword_category
        ,total
        ,"Date Researched" AS date_researched
        ,"6sense MID" AS sixsense_mid
        ,"6sense Account Name" AS sixsense_account_name
        ,"6sense Account Country" AS sixsense_account_country
        ,"6sense Account Domain" AS sixsense_account_domain
    FROM cte_source
)

SELECT 
    *
    ,CURRENT_TIMESTAMP() AS load_date
    ,CURRENT_ROLE() AS dbt_loaded_by
FROM cte_transformed