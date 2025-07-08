{{ config(materialized='view') }}

-- Cleaned and standardized Wealthsimple transactions
select 
    -- Generate unique transaction ID
    {{ dbt_utils.generate_surrogate_key(['row_hash', 'file_name']) }} as transaction_id,
    
    -- Institution identifier
    'wealthsimple' as institution,
    
    -- Standardized transaction fields
    date as transaction_date,
    trim(upper(description)) as description,
    amount,  -- Wealthsimple amounts are already signed correctly
    
    -- Determine transaction type based on Wealthsimple codes
    case 
        when transaction in ('INT', 'CASHBACK', 'AFT_IN') then 'credit'
        when transaction in ('SPEND', 'E_TRFOUT', 'AFT_OUT') then 'debit'
        else 'other'
    end as transaction_type,
    
    -- Institution-specific fields  
    null as merchant,  -- Wealthsimple doesn't separate merchant
    null as merchant_address,
    balance,
    null as foreign_spend_amount,  -- Wealthsimple doesn't track foreign amounts
    
    -- File metadata
    file_name as source_file,
    row_hash as source_row_hash,
    created_at as processed_at
    
from {{ ref('raw_wealthsimple_transactions') }}
where row_num = 1  -- Deduplicate using row_number 