{{ config(materialized='view') }}

-- Raw Wealthsimple transactions with deduplication  
-- This model adds row numbers for deduplication based on content hash
select 
    -- File metadata
    file_name,
    file_hash,
    upload_timestamp,
    processed_timestamp,
    
    -- Original Wealthsimple columns
    date,
    transaction,
    description,
    amount,
    balance,
    
    -- Processing metadata
    row_hash,
    is_processed,
    created_at,
    
    -- Add row number for deduplication
    row_number() over (
        partition by row_hash 
        order by created_at asc
    ) as row_num

from {{ source('personal_finance', 'raw_wealthsimple_transactions') }} 