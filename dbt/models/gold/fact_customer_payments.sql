{{
  config(
    materialized='table',
    schema='gold',
    tags=['gold', 'fact'],
    unique_key='payment_key'
  )
}}

-- STAR SCHEMA FACT TABLE
-- Joins to dimension tables using ref() for foreign keys

with payments as (
    select * from {{ ref('silver_payments') }}
),
dim_customer_current as (
    -- Join to CURRENT version of SCD2 dimension only
    select CUSTOMER_KEY, CUSTOMER_NUMBER 
    from {{ ref('dim_customer_scd2') }}
    where IS_CURRENT = TRUE
),
dim_date as (
    select DATE_KEY, DATE_DAY from {{ ref('dim_date') }}
)
select
    -- Fact surrogate key
    {{ surrogate_key(['p.CUSTOMER_NUMBER', 'p.CHECK_NUMBER']) }} as PAYMENT_KEY,
    
    -- Degenerate dimension (natural key for operational queries)
    p.CHECK_NUMBER,
    
    -- Foreign keys to dimension tables (star schema joins)
    dc.CUSTOMER_KEY,
    coalesce(dd.DATE_KEY, to_number(to_char(p.PAYMENT_DATE, 'YYYYMMDD'))) as PAYMENT_DATE_KEY,
    
    -- Measures (numeric facts)
    p.AMOUNT,
    
    -- Categorical attribute
    p.PAYMENT_TYPE,
    
    -- Date for reference (kept for ease of use)
    p.PAYMENT_DATE,
    
    current_timestamp() as DBT_UPDATED_AT
from payments p
inner join dim_customer_current dc
    on p.CUSTOMER_NUMBER = dc.CUSTOMER_NUMBER
left join dim_date dd
    on dd.DATE_DAY = p.PAYMENT_DATE
