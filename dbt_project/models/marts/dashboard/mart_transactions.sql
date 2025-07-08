{{ config(materialized='table') }}

-- Dashboard-ready transaction data with calculated fields
select 
    transaction_id,
    institution,
    transaction_date,
    description,
    amount,
    abs(amount) as abs_amount,  -- For easier aggregations
    transaction_type,
    merchant,
    merchant_address,
    balance,
    foreign_spend_amount,
    general_category,
    detailed_category,
    category_confidence,
    
    -- Date dimensions for reporting
    extract(year from transaction_date) as transaction_year,
    extract(month from transaction_date) as transaction_month,
    extract(day from transaction_date) as transaction_day,
    extract(dayofweek from transaction_date) as transaction_dow,
    format_date('%Y-%m', transaction_date) as year_month,
    format_date('%Y-Q%Q', transaction_date) as year_quarter,
    
    -- Spending indicators
    case when amount < 0 then abs(amount) else 0 end as spend_amount,
    case when amount > 0 then amount else 0 end as income_amount,
    
    -- Metadata
    source_file,
    source_row_hash,
    processed_at

from {{ ref('int_transactions_enriched') }} 