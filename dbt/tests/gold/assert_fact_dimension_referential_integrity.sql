-- ============================================================================
-- TEST: Fact-Dimension Referential Integrity
-- ============================================================================
-- Layer: Gold (Data Quality - Consistency)
-- Type: Singular Test (Custom SQL)
-- Dimension: Consistency - "Do facts properly reference dimensions?"
-- 
-- WHAT THIS TESTS:
--   - Every CUSTOMER_KEY in fact_order_items exists in dim_customer_scd2
--   - No orphaned fact records
--   - Star schema integrity is maintained
--
-- WHY THIS MATTERS:
--   - Orphaned facts = lost data in reports
--   - JOIN failures = NULL customer names in dashboards
--   - Data quality issues visible to business users
--
-- WHEN IT FAILS:
--   - Fact loaded before dimension
--   - SCD2 snapshot didn't capture all customers
--   - Key generation logic mismatch
--
-- HOW TO DEBUG:
--   1. Find orphaned CUSTOMER_KEY values
--   2. Check if customer exists in Silver layer
--   3. Verify snapshot captured the customer
-- ============================================================================

SELECT
    f.ORDER_ITEM_KEY,
    f.ORDER_NUMBER,
    f.CUSTOMER_KEY
FROM {{ ref('fact_order_items') }} f
LEFT JOIN {{ ref('dim_customer_scd2') }} d
    ON f.CUSTOMER_KEY = d.CUSTOMER_KEY
WHERE d.CUSTOMER_KEY IS NULL

-- Returns orphaned fact records (test fails)
-- Returns 0 rows if all facts reference valid dimensions (test passes)
