-- ============================================================================
-- TEST: Order Revenue Must Be Positive
-- ============================================================================
-- Layer: Silver (Data Quality - Validity)
-- Type: Singular Test (Custom SQL)
-- Dimension: Validity - "Does data conform to business rules?"
-- 
-- WHAT THIS TESTS:
--   - Line revenue (quantity × price) is always positive
--   - No negative quantities (returns should be separate)
--   - No negative prices (discounts handled differently)
--
-- WHEN IT FAILS:
--   - Data corruption in source system
--   - Incorrect transformation logic
--   - Returns mixed in with orders
--
-- HOW TO DEBUG:
--   1. Identify failing ORDER_NUMBER values
--   2. Check QUANTITY_ORDERED and PRICE_EACH in Bronze
--   3. Verify business logic for handling returns/refunds
--
-- BUSINESS IMPACT IF IGNORED:
--   - Incorrect revenue calculations
--   - Financial reporting errors
--   - Compliance issues (SOX)
-- ============================================================================

SELECT
    ORDER_NUMBER,
    PRODUCT_CODE,
    QUANTITY_ORDERED,
    PRICE_EACH,
    LINE_REVENUE
FROM {{ ref('silver_order_items') }}
WHERE LINE_REVENUE <= 0
   OR QUANTITY_ORDERED <= 0
   OR PRICE_EACH <= 0

-- Returns rows with invalid revenue (test fails)
-- Returns 0 rows if all revenue is positive (test passes)
