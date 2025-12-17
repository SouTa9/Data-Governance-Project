{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'dimension', 'scd2']
    )
}}

-- ============================================================================
-- SCD TYPE 2 PRODUCT DIMENSION
-- Purpose: Business-ready product dimension with price/inventory history
-- Source: products_snapshot (SNAPSHOTS schema)
-- 
-- SCD2 History: Each product can have multiple rows representing different
--               versions over time (price changes, stock updates, etc.)
-- ============================================================================

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['PRODUCT_CODE', 'DBT_VALID_FROM']) }} AS PRODUCT_KEY,
    
    -- Business Key
    PRODUCT_CODE,
    
    -- Product Attributes
    PRODUCT_NAME,
    PRODUCT_LINE,
    PRODUCT_SCALE,
    PRODUCT_VENDOR,
    PRODUCT_DESCRIPTION,
    
    -- Inventory & Pricing (SCD Type 2 tracked)
    QUANTITY_IN_STOCK,
    BUY_PRICE,
    MSRP,
    
    -- Derived: Profit Margin
    ROUND((MSRP - BUY_PRICE) / NULLIF(MSRP, 0) * 100, 2) AS PROFIT_MARGIN_PCT,
    
    -- Derived: Price Category
    CASE 
        WHEN MSRP >= 100 THEN 'Premium'
        WHEN MSRP >= 50 THEN 'Standard'
        ELSE 'Budget'
    END AS PRICE_CATEGORY,
    
    -- Derived: Stock Status
    CASE 
        WHEN QUANTITY_IN_STOCK = 0 THEN 'Out of Stock'
        WHEN QUANTITY_IN_STOCK < 100 THEN 'Low Stock'
        WHEN QUANTITY_IN_STOCK < 500 THEN 'Normal Stock'
        ELSE 'High Stock'
    END AS STOCK_STATUS,
    
    -- SCD Type 2 Columns
    DBT_VALID_FROM AS VALID_FROM,
    COALESCE(DBT_VALID_TO, '9999-12-31'::TIMESTAMP) AS VALID_TO,
    CASE WHEN DBT_VALID_TO IS NULL THEN TRUE ELSE FALSE END AS IS_CURRENT,
    
    -- Row Version
    ROW_NUMBER() OVER (
        PARTITION BY PRODUCT_CODE 
        ORDER BY DBT_VALID_FROM
    ) AS VERSION_NUMBER,
    
    -- Audit Columns
    _LOADED_AT AS SOURCE_LOADED_AT,
    _SOURCE_TABLE AS SOURCE_TABLE,
    DBT_UPDATED_AT AS SNAPSHOT_UPDATED_AT,
    CURRENT_TIMESTAMP() AS DBT_UPDATED_AT

FROM {{ ref('products_snapshot') }}
