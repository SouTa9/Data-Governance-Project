{{config(
    materialized='table',
    schema='gold',
    tags=['gold', 'fact'],
    unique_key='order_item_key'
  )
}}

-- STAR SCHEMA FACT TABLE
-- Joins to dimension tables using ref() for foreign keys

with items as (
    select * from {{ ref('silver_order_items') }}
),
orders as (
    select * from {{ ref('silver_orders') }}
    where not IS_CANCELLED
),
dim_customer_current as (
    -- Join to CURRENT version of SCD2 dimension only
    select CUSTOMER_KEY, CUSTOMER_NUMBER 
    from {{ ref('dim_customer_scd2') }}
    where IS_CURRENT = TRUE
),
dim_product_current as (
    -- Join to CURRENT version of SCD2 dimension only
    select PRODUCT_KEY, PRODUCT_CODE, BUY_PRICE 
    from {{ ref('dim_product_scd2') }}
    where IS_CURRENT = TRUE
),
dim_date as (
    select DATE_KEY, DATE_DAY from {{ ref('dim_date') }}
)
select
    -- Fact surrogate key
    {{ surrogate_key(['i.ORDER_NUMBER', 'i.ORDER_LINE_NUMBER']) }} as ORDER_ITEM_KEY,
    
    -- Degenerate dimensions (natural keys stored in fact for operational queries)
    o.ORDER_NUMBER,
    i.ORDER_LINE_NUMBER,
    
    -- Foreign keys to dimension tables (star schema joins)
    dc.CUSTOMER_KEY,
    dp.PRODUCT_KEY,
    
    -- Date foreign keys (role-playing dimension)
    coalesce(dd_order.DATE_KEY, to_number(to_char(o.ORDER_DATE, 'YYYYMMDD'))) as ORDER_DATE_KEY,
    coalesce(dd_required.DATE_KEY, to_number(to_char(o.REQUIRED_DATE, 'YYYYMMDD'))) as REQUIRED_DATE_KEY,
    coalesce(dd_shipped.DATE_KEY, to_number(to_char(o.SHIPPED_DATE, 'YYYYMMDD'))) as SHIPPED_DATE_KEY,
    
    -- Measures (numeric facts)
    i.QUANTITY_ORDERED,
    i.PRICE_EACH,
    i.LINE_REVENUE,
    
    -- Calculated measures (enriched from dimension)
    dp.BUY_PRICE,
    i.QUANTITY_ORDERED * dp.BUY_PRICE as LINE_COST,
    i.QUANTITY_ORDERED * (i.PRICE_EACH - dp.BUY_PRICE) as LINE_PROFIT,
    round({{ safe_divide('i.QUANTITY_ORDERED * (i.PRICE_EACH - dp.BUY_PRICE)', 'nullif(i.LINE_REVENUE, 0)', 'null') }} * 100, 2) as LINE_MARGIN_PCT,
    
    -- Derived measures
    datediff(day, o.ORDER_DATE, coalesce(o.SHIPPED_DATE, current_date)) as DAYS_TO_SHIP,
    
    -- Status attributes
    o.STATUS as ORDER_STATUS,
    o.IS_LATE,
    o.IS_OPEN_ORDER,
    
    -- Dates for reference (kept for ease of use)
    o.ORDER_DATE,
    o.REQUIRED_DATE,
    o.SHIPPED_DATE,
    
    current_timestamp() as DBT_UPDATED_AT
from items i
inner join orders o
    on i.ORDER_NUMBER = o.ORDER_NUMBER
inner join dim_customer_current dc
    on o.CUSTOMER_NUMBER = dc.CUSTOMER_NUMBER
inner join dim_product_current dp
    on i.PRODUCT_CODE = dp.PRODUCT_CODE
left join dim_date dd_order
    on dd_order.DATE_DAY = o.ORDER_DATE
left join dim_date dd_required
    on dd_required.DATE_DAY = o.REQUIRED_DATE
left join dim_date dd_shipped
    on dd_shipped.DATE_DAY = o.SHIPPED_DATE
