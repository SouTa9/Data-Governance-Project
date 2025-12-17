{% snapshot products_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='PRODUCT_CODE',
        strategy='check',
        check_cols=[
            'PRODUCT_NAME',
            'PRODUCT_LINE',
            'PRODUCT_SCALE',
            'PRODUCT_VENDOR',
            'PRODUCT_DESCRIPTION',
            'QUANTITY_IN_STOCK',
            'BUY_PRICE',
            'MSRP'
        ],
        invalidate_hard_deletes=true
    )
}}

-- ============================================================================
-- PRODUCTS SNAPSHOT - SCD TYPE 2 HISTORY TRACKING
-- Purpose: Track price changes, inventory changes, product updates
-- Strategy: Check columns for any changes
-- ============================================================================

SELECT
    -- Business Key
    PRODUCT_CODE,
    
    -- Product Information
    PRODUCT_NAME,
    PRODUCT_LINE,
    PRODUCT_SCALE,
    PRODUCT_VENDOR,
    PRODUCT_DESCRIPTION,
    
    -- Inventory & Pricing (tracked for SCD Type 2)
    QUANTITY_IN_STOCK,
    BUY_PRICE,
    MSRP,
    PROFIT_MARGIN_PCT,
    STOCK_STATUS,
    
    -- Audit Columns
    _LOADED_AT,
    _SOURCE_TABLE
 -- ✅ Read from SILVER (cleaned), not Bronze!

{% endsnapshot %}
