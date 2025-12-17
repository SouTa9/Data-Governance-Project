-- ============================================================================
-- TEST: Order Date Logic Is Valid
-- ============================================================================
-- Layer: Silver (Data Quality - Validity)
-- Type: Singular Test (Custom SQL)
-- Dimension: Validity - "Are order dates logically consistent?"
-- 
-- WHAT THIS TESTS:
--   - ORDER_DATE is not in the future
--   - REQUIRED_DATE is after ORDER_DATE
--   - SHIPPED_DATE (if exists) is after ORDER_DATE
--   - All dates are within realistic business range (2003-2025)
--
-- BUSINESS RULES:
--   - Cannot order in the future (data entry error)
--   - Customer cannot require delivery before ordering
--   - Cannot ship before order is placed
--   - Business started in 2003, future dates max 1 year out
--
-- WHEN IT FAILS:
--   - Future order dates: System clock issue or data entry error
--   - Required < Order: Data entry error
--   - Shipped < Order: Data corruption
--   - Dates outside range: Migration error
--
-- HOW TO DEBUG:
--   1. Check failing ORDER_NUMBER
--   2. Verify dates in Bronze source
--   3. Check ETL timestamp vs order dates
--   4. Review with business for legitimate backdated orders
-- ============================================================================

SELECT
    ORDER_NUMBER,
    ORDER_DATE,
    REQUIRED_DATE,
    SHIPPED_DATE,
    STATUS
FROM {{ ref('silver_orders') }}
WHERE 
    -- Future orders (beyond 1 year)
    ORDER_DATE > DATEADD(year, 1, CURRENT_DATE)
    -- Required date before order date
    OR REQUIRED_DATE < ORDER_DATE
    -- Shipped before ordered
    OR (SHIPPED_DATE IS NOT NULL AND SHIPPED_DATE < ORDER_DATE)
    -- Dates outside business range
    OR ORDER_DATE < '2003-01-01'::DATE
    OR ORDER_DATE > '2025-12-31'::DATE

-- Returns rows with invalid date logic (test fails)
-- Returns 0 rows if all date logic is valid (test passes)
