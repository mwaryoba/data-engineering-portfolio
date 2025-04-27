WITH 
cte_source AS (

    SELECT * 
    FROM {{ ref('stg_sixsense_intent') }}

)

SELECT 
    crm_account_id
    ,crm_account_name
    ,crm_account_country
    ,crm_account_domain
    ,keyword_researched
    ,keyword_category
    ,total
    ,date_researched
    ,sixsense_mid
    ,sixsense_account_name
    ,sixsense_account_country
    ,sixsense_account_domain
    ,load_date
    ,dbt_loaded_by
FROM cte_source