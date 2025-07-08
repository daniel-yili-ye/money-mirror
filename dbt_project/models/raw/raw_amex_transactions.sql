{{ config(materialized='view') }}

-- Raw AmEx transactions with deduplication
-- This model adds row numbers for deduplication based on content hash
select 
    -- File metadata
    file_name,
    file_hash,
    upload_timestamp,
    processed_timestamp,
    
    -- Original AmEx columns
    date,
    date_processed,
    description,
    cardmember,
    amount,
    foreign_spend_amount,
    commission,
    exchange_rate,
    merchant,
    merchant_address,
    additional_information,
    
    -- Processing metadata
    row_hash,
    is_processed,
    created_at,
    
    -- Add row number for deduplication
    row_number() over (
        partition by row_hash 
        order by created_at asc
    ) as row_num

from {{ source('personal_finance', 'raw_amex_transactions') }} 