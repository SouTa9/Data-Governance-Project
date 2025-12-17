-- ============================================================================
-- TEST: Product Prices Are Logically Valid
-- ============================================================================
-- Layer: Gold (Data Quality - Validity)
-- Type: Singular Test (Custom SQL)
-- Dimension: Validity - "Are product prices realistic?"
-- 
-- WHAT THIS TESTS:
--   - BUY_PRICE (cost) is positive
--   - MSRP (selling price) is positive
--   - MSRP is greater than BUY_PRICE (we don't sell at loss)
--   - Prices are within realistic range ($1 to $10,000)
--
-- BUSINESS RULES:
--   - Minimum price: $1 (no free products)
--   - Maximum price: $10,000 (highest product line)
--   - MSRP must be >= BUY_PRICE (otherwise negative margin)
--   - Price ratio: MSRP/BUY_PRICE should be between 1.0 and 10.0
--
-- WHEN IT FAILS:
--   - Zero/negative prices: Data corruption
--   - MSRP < BUY_PRICE: Pricing error (loss maker)
--   - Prices outside range: Data entry error
--   - Extreme ratios: Pricing anomaly
--
-- HOW TO DEBUG:
--   1. Check failing PRODUCT_CODE
--   2. Verify prices in Bronze products source
--   3. Check for data migration issues
--   4. Consult with pricing team for clearance items
-- ============================================================================

SELECT
    PRODUCT_KEY,
    PRODUCT_CODE,
    PRODUCT_NAME,
    BUY_PRICE,
    MSRP,
    ROUND(MSRP / NULLIF(BUY_PRICE, 0), 2) AS PRICE_RATIO
FROM {{ ref('dim_product_scd2') }}
WHERE 
    -- Invalid BUY_PRICE
    BUY_PRICE <= 0
    -- Invalid MSRP
    OR MSRP <= 0
    -- Selling at loss (MSRP < cost)
    OR MSRP < BUY_PRICE
    -- Prices outside realistic range
    OR BUY_PRICE > 10000
    OR MSRP > 10000
    -- Extreme price ratios (markup too high)
    OR (MSRP / NULLIF(BUY_PRICE, 0)) > 10

-- Returns rows with invalid pricing logic (test fails)
-- Returns 0 rows if all prices are valid (test passes)
