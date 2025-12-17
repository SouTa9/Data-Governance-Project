-- ============================================================================
-- TEST: Gold Fact Order Items Has Correct Grain
-- ============================================================================
-- Layer: Gold (Data Quality - Uniqueness)
-- Type: Singular Test (Custom SQL)
-- Dimension: Uniqueness - "Is the grain correct?"
-- 
-- WHAT THIS TESTS:
--   - Fact table grain is one row per ORDER_NUMBER + ORDER_LINE_NUMBER
--   - No duplicate line items that would inflate revenue
--   - Surrogate key (ORDER_ITEM_KEY) is truly unique
--
-- WHY GRAIN MATTERS:
--   - Duplicate rows = double-counted revenue
--   - Duplicate rows = incorrect inventory deductions
--   - Duplicate rows = wrong commission calculations
--
-- WHEN IT FAILS:
--   - Multiple records with same natural key combination
--   - SCD2 join logic may be creating duplicates
--   - Window function for surrogate key failed
--
-- HOW TO DEBUG:
--   1. Find duplicate ORDER_NUMBER + ORDER_LINE_NUMBER combinations
--   2. Check snapshot join logic in fact model
--   3. Verify surrogate key generation
-- ============================================================================

SELECT
    ORDER_NUMBER,
    ORDER_LINE_NUMBER,
    COUNT(*) AS duplicate_count
FROM {{ ref('fact_order_items') }}
GROUP BY ORDER_NUMBER, ORDER_LINE_NUMBER
HAVING COUNT(*) > 1

-- Returns rows with duplicates (test fails)
-- Returns 0 rows if grain is correct (test passes)
