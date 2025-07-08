{{ config(materialized='view') }}

-- Union all institution transactions into standardized format
select * from {{ ref('stg_amex_transactions') }}

union all

select * from {{ ref('stg_wealthsimple_transactions') }} 