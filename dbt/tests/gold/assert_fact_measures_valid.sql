-- ============================================================================
-- TEST: Fact Table Measures Are Valid
-- ============================================================================
-- Layer: Gold (Data Quality - Validity)
-- Type: Singular Test (Custom SQL)
-- Dimension: Validity - "Are fact table measures logically valid?"
-- 
-- WHAT THIS TESTS:
--   - QUANTITY_ORDERED is positive (no zero or negative)
--   - PRICE_EACH is positive
--   - LINE_REVENUE is positive and matches calculation
--   - LINE_COST is non-negative
--   - LINE_PROFIT calculation is correct (revenue - cost)
--
-- BUSINESS RULES:
--   - All quantities must be positive integers
--   - All prices must be positive decimals
--   - Revenue = Quantity × Price
--   - Profit = Revenue - Cost
--   - No negative costs (COGS always >= 0)
--
-- WHEN IT FAILS:
--   - Negative values: Data corruption or calculation error
--   - Mismatched calculation: Formula bug in model
--   - Zero values: Data quality issue in source
--
-- HOW TO DEBUG:
--   1. Identify failing ORDER_ITEM_KEY
--   2. Check source values in silver_order_items
--   3. Verify calculation logic in fact_order_items model
--   4. Cross-reference with dim_product_scd2 for BUY_PRICE
--
-- BUSINESS IMPACT IF IGNORED:
--   - Revenue reports incorrect
--   - Profit margin calculations wrong
--   - Executive dashboards show bad data
-- ============================================================================

SELECT
    ORDER_ITEM_KEY,
    ORDER_NUMBER,
    ORDER_LINE_NUMBER,
    QUANTITY_ORDERED,
    PRICE_EACH,
    LINE_REVENUE,
    LINE_COST,
    LINE_PROFIT
FROM {{ ref('fact_order_items') }}
WHERE 
    -- Invalid quantities
    QUANTITY_ORDERED <= 0
    -- Invalid prices
    OR PRICE_EACH <= 0
    -- Invalid revenue
    OR LINE_REVENUE <= 0
    -- Invalid cost (negative cost impossible)
    OR LINE_COST < 0
    -- Revenue calculation mismatch (allow 0.01 rounding difference)
    OR ABS(LINE_REVENUE - (QUANTITY_ORDERED * PRICE_EACH)) > 0.01
    -- Profit calculation mismatch
    OR ABS(LINE_PROFIT - (LINE_REVENUE - LINE_COST)) > 0.01

-- Returns rows with invalid measures (test fails)
-- Returns 0 rows if all measures are valid (test passes)
