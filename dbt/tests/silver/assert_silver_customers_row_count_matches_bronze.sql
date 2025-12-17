-- ============================================================================
-- TEST: Silver Customers Row Count Matches Bronze
-- ============================================================================
-- Layer: Silver (Data Quality - Completeness)
-- Type: Singular Test (Custom SQL)
-- Dimension: Completeness - "Is all expected data present?"
-- 
-- WHAT THIS TESTS:
--   - No data loss during Bronze → Silver transformation
--   - Deduplication logic is correct (should be 1:1 for customers)
--   - Transformation didn't accidentally filter out records
--
-- WHEN IT FAILS:
--   - Silver has FEWER rows: Data was lost/filtered incorrectly
--   - Silver has MORE rows: Deduplication failed, duplicates exist
--
-- HOW TO DEBUG:
--   1. Check Silver deduplication logic (ROW_NUMBER window function)
--   2. Look for multiple records per CUSTOMER_NUMBER in Bronze
--   3. Compare counts: SELECT COUNT(*) FROM bronze.customers vs silver.silver_customers
--
-- BUSINESS IMPACT IF IGNORED:
--   - Missing customers = incomplete revenue reporting
--   - Duplicate customers = inflated customer counts
-- ============================================================================

WITH bronze_count AS (
    SELECT COUNT(*) AS cnt
    FROM {{ source('bronze', 'customers') }}
),
silver_count AS (
    SELECT COUNT(*) AS cnt
    FROM {{ ref('silver_customers') }}
)
SELECT
    b.cnt AS bronze_rows,
    s.cnt AS silver_rows,
    b.cnt - s.cnt AS difference
FROM bronze_count b
CROSS JOIN silver_count s
WHERE b.cnt != s.cnt

-- Returns rows if counts DON'T match (test fails)
-- Returns 0 rows if counts match (test passes)
