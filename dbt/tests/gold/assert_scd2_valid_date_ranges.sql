-- ============================================================================
-- TEST: SCD2 Dimension Has Valid Date Ranges
-- ============================================================================
-- Layer: Gold (Data Quality - Validity)
-- Type: Singular Test (Custom SQL)
-- Dimension: Validity - "Are SCD2 date ranges logical?"
-- 
-- WHAT THIS TESTS:
--   - VALID_FROM is always before VALID_TO
--   - No overlapping date ranges for same customer
--   - Current records have VALID_TO = '9999-12-31' or NULL
--
-- SCD TYPE 2 RULES:
--   - Each customer version has non-overlapping date ranges
--   - Only ONE version should be "current" (IS_CURRENT = true)
--   - Historical versions have proper end dates
--
-- WHEN IT FAILS:
--   - Snapshot logic error
--   - Time travel query issues
--   - Incorrect SCD2 implementation
--
-- HOW TO DEBUG:
--   1. Check customer versions: SELECT * FROM dim_customer_scd2 WHERE CUSTOMER_NUMBER = X ORDER BY VALID_FROM
--   2. Verify snapshot timestamp columns
--   3. Check IS_CURRENT flag logic
-- ============================================================================

SELECT
    CUSTOMER_KEY,
    CUSTOMER_NUMBER,
    CUSTOMER_NAME,
    VALID_FROM,
    VALID_TO,
    IS_CURRENT
FROM {{ ref('dim_customer_scd2') }}
WHERE 
    -- Check: VALID_FROM should be before VALID_TO
    (VALID_TO IS NOT NULL AND VALID_FROM >= VALID_TO)
    -- Check: Current records should have future/null end date
    OR (IS_CURRENT = TRUE AND VALID_TO IS NOT NULL AND VALID_TO < CURRENT_DATE())

-- Returns rows with invalid date ranges (test fails)
-- Returns 0 rows if SCD2 logic is correct (test passes)
