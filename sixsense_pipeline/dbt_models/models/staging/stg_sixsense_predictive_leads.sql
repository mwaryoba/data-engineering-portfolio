WITH 
cte_source AS (
    SELECT *
    FROM {{ source('sixsense', 'raw_predictiveleads') }}
),
cte_transformed AS (
    SELECT 
        "CRM Lead ID" AS crm_lead_id
        ,"CRM Account Name" AS crm_account_name
        ,"CRM Account Country" AS crm_account_country
        ,"CRM Account Domain" AS crm_account_domain
        ,"Product Category" AS product_category
        ,"Buying Stage" AS buying_stage
        ,"Intent Score" AS intent_score
        ,"Profile Fit" AS profile_fit
        ,"Profile Fit Score" AS profile_fit_score
        ,"Contact Intent Grade" AS contact_intent_grade
        ,"Contact Intent Score" AS contact_intent_score
        ,"Contact Profile Fit" AS contact_profile_fit
        ,"Contact Profile Fit Score" AS contact_profile_fit_score
        ,date
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