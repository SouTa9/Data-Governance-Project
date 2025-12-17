-- ============================================================================
-- TEST: Profit Margin is Within Reasonable Range
-- ============================================================================
-- Layer: Gold (Data Quality - Validity)
-- Type: Singular Test (Custom SQL)
-- Dimension: Validity - "Are calculated values reasonable?"
-- 
-- WHAT THIS TESTS:
--   - Profit margin percentage is between -100% and +200%
--   - No mathematical errors in calculation
--   - Business rules for pricing are followed
--
-- BUSINESS RULES:
--   - Margin below -50%: Unusual loss leader pricing
--   - Margin above 95%: Likely data error (200%+ markup rare in wholesale)
--   - Typical range: -10% to 80%
--   - NULL margin: Missing cost data (handled by not_null test)
--
-- WHEN IT FAILS:
--   - Division by zero in calculation
--   - Incorrect BUY_PRICE or MSRP in source
--   - Product pricing anomaly
--
-- HOW TO DEBUG:
--   1. Check failing PRODUCT_CODE values
--   2. Verify BUY_PRICE and MSRP in Bronze source
--   3. Review margin calculation: (MSRP - BUY_PRICE) / MSRP * 100
--   4. Check if product is discontinued (may have unusual pricing)
-- ============================================================================

SELECT
    PRODUCT_KEY,
    PRODUCT_CODE,
    PRODUCT_NAME,
    BUY_PRICE,
    MSRP,
    PROFIT_MARGIN_PCT
FROM {{ ref('dim_product_scd2') }}
WHERE PROFIT_MARGIN_PCT IS NOT NULL
  AND (PROFIT_MARGIN_PCT < -50 OR PROFIT_MARGIN_PCT > 95)

-- Returns rows with unreasonable margins (test fails)
-- Returns 0 rows if margins are reasonable (test passes)
