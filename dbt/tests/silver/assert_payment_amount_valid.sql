-- ============================================================================
-- TEST: Payment Amounts Are Valid
-- ============================================================================
-- Layer: Silver (Data Quality - Validity)
-- Type: Singular Test (Custom SQL)
-- Dimension: Validity - "Are payment amounts reasonable?"
-- 
-- WHAT THIS TESTS:
--   - Payment amounts are not zero
--   - Refunds (negative amounts) are within reasonable limits
--   - Payment amounts don't exceed maximum transaction limit
--
-- BUSINESS RULES:
--   - Positive payments: $0.01 to $150,000 (max order value)
--   - Refunds (negative): -$150,000 to -$0.01
--   - Zero payments are invalid (data entry error)
--
-- WHEN IT FAILS:
--   - Zero amount: Data entry error or null conversion issue
--   - Exceeds max: Possible fraud or data corruption
--   - Refund too large: Validation bypass
--
-- HOW TO DEBUG:
--   1. Identify failing CHECK_NUMBER and CUSTOMER_NUMBER
--   2. Check source system payment records
--   3. Verify with finance team for legitimate large transactions
--   4. Review payment approval workflow
--
-- BUSINESS IMPACT IF IGNORED:
--   - Revenue calculation errors
--   - Financial statement inaccuracies
--   - Potential fraud undetected
-- ============================================================================

SELECT
    CUSTOMER_NUMBER,
    CHECK_NUMBER,
    PAYMENT_DATE,
    AMOUNT,
    PAYMENT_TYPE
FROM {{ ref('silver_payments') }}
WHERE AMOUNT = 0
   OR AMOUNT > 150000
   OR AMOUNT < -150000

-- Returns rows with invalid payment amounts (test fails)
-- Returns 0 rows if all payments are valid (test passes)
