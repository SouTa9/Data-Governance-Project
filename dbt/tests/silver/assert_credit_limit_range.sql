-- ============================================================================
-- TEST: Credit Limit is Within Business Range
-- ============================================================================
-- Layer: Silver (Data Quality - Validity)
-- Type: Singular Test (Custom SQL)
-- Dimension: Validity - "Is data within acceptable range?"
-- 
-- WHAT THIS TESTS:
--   - Credit limits are non-negative (no negative credit)
--   - Credit limits don't exceed maximum allowed ($500,000)
--   - Business policy compliance
--
-- BUSINESS RULES:
--   - Minimum credit limit: $0 (prepayment customers)
--   - Maximum credit limit: $500,000 (requires VP approval above this)
--
-- WHEN IT FAILS:
--   - Negative credit limit: Data corruption or entry error
--   - Exceeds max: Approval bypass or data entry error
--
-- HOW TO DEBUG:
--   1. Identify failing CUSTOMER_NUMBER values
--   2. Check source system for credit approval records
--   3. Verify business rule hasn't changed
-- ============================================================================

SELECT
    CUSTOMER_NUMBER,
    CUSTOMER_NAME,
    CREDIT_LIMIT
FROM {{ ref('silver_customers') }}
WHERE CREDIT_LIMIT IS NOT NULL
  AND (CREDIT_LIMIT < 0 OR CREDIT_LIMIT > 500000)

-- Returns rows with out-of-range credit limits (test fails)
-- Returns 0 rows if all credit limits are valid (test passes)
