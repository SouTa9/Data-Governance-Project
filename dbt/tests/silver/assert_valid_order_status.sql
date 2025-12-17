-- ============================================================================
-- TEST: Valid Order Status Values
-- ============================================================================
-- Layer: Silver (Data Quality - Validity)
-- Type: Singular Test (Custom SQL)
-- Dimension: Validity - "Is data within expected domain?"
-- 
-- WHAT THIS TESTS:
--   - Order status is one of the allowed business values
--   - No typos or unexpected status codes
--   - Data conforms to business process definition
--
-- ALLOWED VALUES:
--   - 'Shipped': Order delivered to customer
--   - 'Resolved': Order completed/closed
--   - 'Cancelled': Order cancelled by customer/company
--   - 'On Hold': Order pending review
--   - 'Disputed': Order under dispute
--   - 'In Process': Order being processed
--
-- WHEN IT FAILS:
--   - New status added in source without documentation
--   - Data entry error in source system
--   - Case sensitivity issues
--
-- HOW TO DEBUG:
--   1. Check unique STATUS values in Bronze
--   2. Verify case matches expected values
--   3. Update allowed values if business process changed
-- ============================================================================

SELECT
    ORDER_NUMBER,
    STATUS,
    ORDER_DATE
FROM {{ ref('silver_orders') }}
WHERE STATUS NOT IN (
    'SHIPPED',
    'RESOLVED', 
    'CANCELLED',
    'ON HOLD',
    'DISPUTED',
    'IN PROCESS'
)

-- Returns rows with invalid status (test fails)
-- Returns 0 rows if all statuses are valid (test passes)
