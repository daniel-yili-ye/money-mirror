{{ config(materialized='view') }}

-- Enrich transactions with category data from Gemini
select 
    t.transaction_id,
    t.institution,
    t.transaction_date,
    t.description,
    t.amount,
    t.transaction_type,
    t.merchant,
    t.merchant_address,
    t.balance,
    t.foreign_spend_amount,
    
    -- Add category enrichment from Gemini cache
    coalesce(cat.general_category, 'Uncategorized') as general_category,
    coalesce(cat.detailed_category, 'Uncategorized') as detailed_category,
    cat.confidence_score as category_confidence,
    
    -- Metadata
    t.source_file,
    t.source_row_hash,
    t.processed_at

from {{ ref('stg_all_transactions') }} t
left join {{ source('personal_finance', 'dim_description_categories') }} cat
    on upper(trim(t.description)) = upper(trim(cat.description_key)) 