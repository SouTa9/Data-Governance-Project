-- ============================================================================
-- TEST: Product SCD2 Has Exactly One Current Version Per Product
-- ============================================================================
-- Layer: Gold (Data Quality - SCD2 Validation)
-- Type: Singular Test (Custom SQL)
-- Dimension: Uniqueness - "Is there exactly one current version?"
-- 
-- WHAT THIS TESTS:
--   - Each product (PRODUCT_CODE) has exactly ONE row with IS_CURRENT = TRUE
--   - No products have zero current versions (missing data)
--   - No products have multiple current versions (deduplication bug)
--
-- BUSINESS RULES:
--   - IS_CURRENT = TRUE means this is the active product version
--   - Only ONE version can be current at any time
--   - All products must have a current version
--
-- WHEN IT FAILS:
--   - 0 current versions: Product deleted or SCD2 logic bug
--   - 2+ current versions: Deduplication failed in snapshot
--
-- HOW TO DEBUG:
--   1. Identify failing PRODUCT_CODE
--   2. Query dim_product_scd2 for all versions:
--      SELECT * FROM dim_product_scd2 WHERE PRODUCT_CODE = 'XXX' ORDER BY VALID_FROM
--   3. Check snapshot logic in snapshots/scd_product.sql
--   4. Verify VALID_TO is NULL for current version only
--
-- BUSINESS IMPACT IF IGNORED:
--   - BI dashboards show wrong product data
--   - Fact table joins return duplicates or nulls
--   - Revenue calculations incorrect
-- ============================================================================

SELECT
    PRODUCT_CODE,
    COUNT(*) AS current_version_count
FROM {{ ref('dim_product_scd2') }}
WHERE IS_CURRENT = TRUE
GROUP BY PRODUCT_CODE
HAVING COUNT(*) != 1

-- Returns products with != 1 current version (test fails)
-- Returns 0 rows if all products have exactly 1 current version (test passes)
