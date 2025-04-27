WITH 
cte_source AS (

    SELECT * 
    FROM {{ ref('stg_sixsense_predictive_leads') }}

)

SELECT 
    crm_lead_id
    ,crm_account_name
    ,crm_account_country
    ,crm_account_domain
    ,product_category
    ,buying_stage
    ,intent_score
    ,profile_fit
    ,profile_fit_score
    ,contact_intent_grade
    ,contact_intent_score
    ,contact_profile_fit
    ,contact_profile_fit_score
    ,date
    ,sixsense_mid
    ,sixsense_account_name
    ,sixsense_account_country
    ,sixsense_account_domain
    ,load_date
    ,dbt_loaded_by
FROM cte_source