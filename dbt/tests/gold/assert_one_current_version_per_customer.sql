-- ============================================================================
-- TEST: Each Customer Has Exactly One Current Version
-- ============================================================================
-- Layer: Gold (Data Quality - Uniqueness)
-- Type: Singular Test (Custom SQL)
-- Dimension: Uniqueness - "Is there exactly one current record per entity?"
-- 
-- WHAT THIS TESTS:
--   - Each CUSTOMER_NUMBER has exactly ONE row where IS_CURRENT = true
--   - No customers with ZERO current versions (orphaned history)
--   - No customers with MULTIPLE current versions (SCD2 error)
--
-- WHY THIS MATTERS:
--   - Fact tables join to current dimension records
--   - Multiple current = duplicate fact rows after join
--   - Zero current = fact rows with NULL dimensions
--
-- WHEN IT FAILS:
--   - 0 current versions: Customer was soft-deleted incorrectly
--   - 2+ current versions: Snapshot didn't close previous version
--
-- HOW TO DEBUG:
--   1. SELECT * FROM dim_customer_scd2 WHERE CUSTOMER_NUMBER = X ORDER BY VALID_FROM
--   2. Check DBT_VALID_TO in snapshots table
--   3. Verify dimension model IS_CURRENT logic
-- ============================================================================

WITH current_version_counts AS (
    SELECT
        CUSTOMER_NUMBER,
        SUM(CASE WHEN IS_CURRENT = TRUE THEN 1 ELSE 0 END) AS current_count
    FROM {{ ref('dim_customer_scd2') }}
    GROUP BY CUSTOMER_NUMBER
)
SELECT
    CUSTOMER_NUMBER,
    current_count
FROM current_version_counts
WHERE current_count != 1

-- Returns customers with wrong number of current versions (test fails)
-- Returns 0 rows if each customer has exactly one current version (test passes)
