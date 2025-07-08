{{ config(materialized='view') }}

-- Cleaned and standardized AmEx transactions
select 
    -- Generate unique transaction ID
    {{ dbt_utils.generate_surrogate_key(['row_hash', 'file_name']) }} as transaction_id,
    
    -- Institution identifier
    'amex' as institution,
    
    -- Standardized transaction fields
    date as transaction_date,
    trim(upper(description)) as description,
    amount * -1 as amount,  -- AmEx shows purchases as positive, normalize to negative
    
    -- Determine transaction type
    case 
        when amount > 0 then 'credit'  -- Payment or refund
        when amount < 0 then 'debit'   -- Purchase
        else 'other'
    end as transaction_type,
    
    -- Institution-specific fields
    trim(merchant) as merchant,
    trim(merchant_address) as merchant_address,
    null as balance,  -- AmEx doesn't have running balance
    foreign_spend_amount,
    
    -- File metadata
    file_name as source_file,
    row_hash as source_row_hash,
    created_at as processed_at
    
from {{ ref('raw_amex_transactions') }}
where row_num = 1  -- Deduplicate using row_number 