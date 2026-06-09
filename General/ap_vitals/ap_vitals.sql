-- ============================================================
-- SQL QUERY OPTIMIZATION REPORT
-- Original query: Complex observation retrieval with external codes and lab panels
-- Generated: May 22, 2026
-- Engine: MySQL
-- ============================================================

-- ============================================================
-- SECTION 1: EXPLAIN ANALYSIS SUMMARY
-- ============================================================
-- 
-- CRITICAL PERFORMANCE ISSUE IDENTIFIED:
-- Query performs FULL TABLE SCANS on 6 major tables with NO index usage.
-- The join strategy relies on HASH JOIN with temporary buffers, which is 
-- extremely expensive for a 33M+ row table (OBS).
--
-- Table: OBS (Primary)
--   Access type: ALL (FULL TABLE SCAN) ❌❌❌
--   Key used: NONE
--   Rows scanned: 33,191,693 (entire table)
--   Filtered: 3.70% (post-scan filter)
--   Issues: 
--     - No index used despite WHERE conditions on PID, XID, CHANGE
--     - 33M rows scanned, only ~1.2M rows pass filter
--     - Dependent subquery re-runs per outer row
--     - WHERE clause includes OR condition that defeats index usage
--
-- Table: PT (PATIENTPROFILE)
--   Access type: ALL (FULL TABLE SCAN) ❌
--   Key used: NONE
--   Rows scanned: 558,878
--   Filtered: 100% (no WHERE, just used in join)
--   Join method: Hash join buffer (expensive)
--   Issues:
--     - No WHERE condition, full table needed
--     - But should join after OBS is filtered, not before
--
-- Table: D (DOCUMENT)
--   Access type: ALL (FULL TABLE SCAN) ❌❌
--   Key used: NONE
--   Rows scanned: 20,750,495 
--   Filtered: 1.00% (almost all rows discarded)
--   Join method: Hash join buffer
--   Issues:
--     - MASSIVE inefficiency: scanning 20M rows to keep ~200K
--     - Joins on SDID but could benefit from index
--
-- Table: CN (CONFTYPES)
--   Access type: ALL (FULL TABLE SCAN) ❌
--   Key used: NONE
--   Rows scanned: 9 (small table, acceptable)
--   Filtered: 100%
--   Issues:
--     - Tiny table, but still a full scan (negligible impact)
--     - Joins on CONFTYPEID
--
-- Table: H (OBSHEAD)
--   Access type: ALL (FULL TABLE SCAN) ❌
--   Key used: NONE
--   Rows scanned: 89,702
--   Filtered: 0.10% (keeps only ~90 rows)
--   Issues:
--     - WHERE H.GROUPID = 1300 should use index
--     - 89K rows scanned to get 90 results is wasteful
--
-- Table: HG (HIERGRPS)
--   Access type: ALL (FULL TABLE SCAN) ❌
--   Key used: NONE
--   Rows scanned: 1,428
--   Filtered: 100%
--   Join method: Hash join buffer FIRST (wrong!)
--   Issues:
--     - Small table, but scanned FIRST in join order
--     - Should be inner join after filtering on OBSHEAD.GROUPID
--
-- Table: R1, R2 (REL_OBS_EXT_CODE)
--   Access type: ALL (FULL TABLE SCAN) ❌❌
--   Key used: NONE (has IX_OBS_active_pk available but not used)
--   Rows scanned: 10,438,717 EACH
--   Join method: Hash join buffer
--   Issues:
--     - LEFT JOINs scanning 10M+ rows each
--     - WHERE conditions (EXT_CODE_ORDER = 1/2) could use index
--     - Could be converted to UNION or conditional aggregation
--
-- Table: C1, C2 (EXT_CODE)
--   Access type: ALL (FULL TABLE SCAN) ❌
--   Key used: NONE
--   Rows scanned: 8,470 EACH
--   Join method: Hash join buffer
--   Issues:
--     - Joins on EXT_CODE_ID, should use index
--
-- Table: LOP (LABORDERPANEL)
--   Access type: ALL (FULL TABLE SCAN) ❌
--   Key used: NONE
--   Rows scanned: 1,344,967
--   Join method: Hash join buffer
--   Issues:
--     - LEFT JOIN on optional LABORDERPANELID
--     - Needs index on LABORDERPANELID
--
-- Subquery: O2 (Dependent subquery)
--   Dependency: Runs ONCE PER OUTER ROW due to OR condition
--   Access type: index (IX_OBS_active_pk)
--   Key used: IX_OBS_active_pk (good!)
--   Rows scanned: 35
--   Filtered: 33M rows estimated, ~1.2M actual
--   Issues:
--     - Correlated NOT EXISTS is executed repeatedly
--     - Could be rewritten as LEFT JOIN with NULL check
--     - Current approach: O.CHANGE IN (0,4,10) AND O.XID != O2.OBSID per outer row
--

-- ============================================================
-- SECTION 2: ROOT CAUSE ANALYSIS
-- ============================================================
--
-- The query has MULTIPLE STRUCTURAL PROBLEMS:
--
-- 1. FILTER ORDER IS BACKWARDS
--    - HG and PT are scanned FIRST (1.4K and 558K rows)
--    - Then OBS is scanned FULL (33M rows)
--    - Correct order: Filter OBS first → join to HG/PT
--
-- 2. NO INDEXES ON JOIN/FILTER COLUMNS
--    Missing critical indexes on:
--    - OBS(PID, XID, CHANGE)
--    - OBSHEAD(GROUPID)
--    - REL_OBS_EXT_CODE(OBSID, EXT_CODE_ORDER)
--    - EXT_CODE(EXT_CODE_ID)
--    - DOCUMENT(SDID, CONFTYPE)
--    - LABORDERPANEL(LABORDERPANELID)
--
-- 3. OR CONDITION DEFEATS INDEX USAGE
--    WHERE (O.XID = 1E18 OR (O.CHANGE IN (0,4,10) AND ...))
--    - Optimizer can't use a single index for OR branches
--    - Should be rewritten as UNION ALL with separate indexes
--
-- 4. CORRELATED SUBQUERY IS INEFFICIENT
--    NOT EXISTS with correlation (O2 depends on O.PID, O.XID)
--    - Runs once per outer row
--    - Could be LEFT JOIN + NULL check (executed once)
--
-- 5. MULTIPLE LEFT JOINs ON LARGE TABLES
--    - R1, R2, C1, C2, LOP are all LEFT JOINs on large tables
--    - If LEFT JOINs are needed for optional data, current approach is OK
--    - But scanning 10M+ rows for 1.2M results is wasteful
--

-- ============================================================
-- SECTION 3: RECOMMENDED INDEXES
-- ============================================================

-- Problem: O.PID used in INNER JOIN with PT, should be indexed
-- Impact: Enables efficient hash join on PID, reduces rows to probe
-- Estimated improvement: 20-30% reduction in join time
CREATE INDEX idx_OBS_PID ON OBS (PID);

-- Problem: O.XID in WHERE clause (one branch of OR condition)
-- Impact: Can satisfy XID = 1E18 without full table scan
-- Estimated improvement: Could reduce scan from 33M to thousands
CREATE INDEX idx_OBS_XID ON OBS (XID);

-- Problem: O.CHANGE in WHERE clause for the second OR branch
-- Impact: Combined with PID, enables efficient filtering
-- Estimated improvement: Reduces filtered rows 3.70% → ~0.1%
CREATE INDEX idx_OBS_CHANGE ON OBS (CHANGE, PID) COMMENT 'ESR rule: CHANGE first (equality), PID second';

-- Problem: O.HDID used in INNER JOIN, and H.GROUPID = 1300 filters down
-- Impact: Join OBSHEAD after pre-filtering by GROUPID
-- Estimated improvement: Reduces 89K rows → 90 rows before OBS join
CREATE INDEX idx_OBSHEAD_GROUPID ON OBSHEAD (GROUPID, HDID) COMMENT 'Covering index for HDID lookup';

-- Problem: REL_OBS_EXT_CODE scanned fully (10M rows) for LEFT JOIN
-- Impact: Index on (OBSID, EXT_CODE_ORDER) enables efficient outer join
-- Estimated improvement: 80%+ reduction in rows examined
CREATE INDEX idx_REL_OBS_EXT_CODE_OBSID ON REL_OBS_EXT_CODE (OBSID, EXT_CODE_ORDER);

-- Problem: DOCUMENT scanned fully (20M rows) for SDID = O.SDID
-- Impact: Index enables efficient join
-- Estimated improvement: 50-70% reduction, avoids 20M row scan
CREATE INDEX idx_DOCUMENT_SDID ON DOCUMENT (SDID, CONFTYPE);

-- Problem: EXT_CODE.EXT_CODE_ID used in LEFT JOIN
-- Impact: Index on primary join key
-- Estimated improvement: Hash join becomes index-based (much faster)
CREATE INDEX idx_EXT_CODE_ID ON EXT_CODE (EXT_CODE_ID);

-- Problem: LABORDERPANEL.LABORDERPANELID used in LEFT JOIN
-- Impact: Enables efficient optional join
-- Estimated improvement: 30-50% reduction for this LEFT JOIN
CREATE INDEX idx_LABORDERPANEL_ID ON LABORDERPANEL (LABORDERPANELID);

-- Optional: Covering index on OBS to avoid table lookups
-- Use ONLY if the 13 columns selected from OBS are frequently accessed
-- Impact: 10-15% additional improvement, but increases INSERT/UPDATE cost
-- CREATE INDEX idx_OBS_PID_covered ON OBS (PID, CHANGE, XID, SDID, HDID, OBSID, OBSVALUE) COMMENT 'Covering index for common selects';


-- ============================================================
-- SECTION 4: QUERY REWRITE STRATEGIES
-- ============================================================
--
-- STRATEGY A: Split OR condition into UNION ALL
-- This allows each branch to use its own index:
--
--   SELECT ... WHERE O.XID = 1E18 AND H.GROUPID = 1300
--   UNION ALL
--   SELECT ... WHERE O.CHANGE IN (0,4,10) AND NOT EXISTS(...) AND H.GROUPID = 1300
--
-- Impact: Enables index on both branches independently
--
-- STRATEGY B: Convert correlated NOT EXISTS to LEFT JOIN + NULL
-- Current: NOT EXISTS (SELECT 1 FROM OBS O2 WHERE O.PID = O2.PID AND O.XID = O2.OBSID)
-- Rewrite:
--   LEFT JOIN OBS O2 ON O.PID = O2.PID AND O.XID = O2.OBSID
--   WHERE O2.OBSID IS NULL
--
-- Impact: Subquery runs once instead of per outer row
--
-- STRATEGY C: Reorder joins to filter early
-- Current: HG → PT → O (33M rows)
-- Better:  H (GROUPID filter) → HG → O → PT → D → ...
--
-- Impact: Reduces join fan-out early
--
-- STRATEGY D: Denormalize or pre-aggregate if this runs frequently
-- If this query runs thousands of times/day:
--   - Create a materialized view of filtered OBS + OBSHEAD
--   - Materialize LEFT JOINs into a fact table
--   - Use incremental refresh if data changes slowly
--


-- ============================================================
-- SECTION 5: OPTIMIZED QUERY (UNION ALL STRATEGY)
-- ============================================================
--
-- Changes made:
--   1. Split OR condition into two branches with UNION ALL
--      - Branch 1: WHERE O.XID = 1E18 (direct lookup, uses idx_OBS_XID)
--      - Branch 2: WHERE O.CHANGE IN (0,4,10) (uses idx_OBS_CHANGE)
--   2. Converted correlated NOT EXISTS to LEFT JOIN + NULL check
--      (no subquery re-execution)
--   3. Added all recommended indexes (listed in Section 3)
--   4. Reordered joins: Filter on H.GROUPID first, then join OBS
--   5. Added column hints to clarify index usage
--
-- Expected improvement: 80-95% reduction in query time
--   - Before: 33M row scans across 10+ tables
--   - After: ~50K-100K rows examined with proper indexes
--

-- ============================================================================
-- OPTIMIZED QUERY - BRANCH 1: Direct XID lookup
-- ============================================================================
SELECT 
    O.OBSID,
    O.PID,
    O.XID,
    O.`CHANGE`,
    O.SDID,
    O.USRID,
    O.HDID,
    O.ABNORMAL,
    O.OBSDATE,
    O.OBSTYPE,
    O.OBSVALUE,
    O.PUBUSER,
    O.PUBTIME,
    O.PARENTID,
    O.`RANGE`,
    O.DESCRIPTION,
    CASE 
        WHEN O.STATE IS NULL OR O.STATE = '' THEN 'F'
        ELSE O.STATE
    END AS STATE,
    O.ENTRYID,
    O.ARCHIVE,
    O.DB_CREATE_DATE,
    O.DB_UPDATED_DATE,
    H.NAME,
    H.UNIT,
    H.DESCRIPTION AS OBSHEAD_DESCRIPTION,
    HG.GROUPNAME AS HG_GROUPNAME,
    HG.GROUPID AS HG_GROUPID,
    PT.SENSITIVECHART,
    C1.CODE AS C1_CODE,
    C1.CODING_SYSTEM_NAME AS C1_CODING_SYSTEM_NAME,
    C1.DESCRIPTION AS C1_DESCRIPTION,
    C2.CODE AS C2_CODE,
    C2.CODING_SYSTEM_NAME AS C2_CODING_SYSTEM_NAME,
    C2.DESCRIPTION AS C2_DESCRIPTION,
    CN.ABBR AS CONFABBR,
    IFNULL(CN.CONFTYPEID, 0) AS CONFTYPEID,
    LOP.CODE AS LOP_CODE,
    LOP.CODETYPE AS LOP_CODETYPE,
    LOP.NAME AS LOP_NAME,
    PT.LOCATIONID AS LOCATIONID
FROM OBS O
  USE INDEX (idx_OBS_XID)  -- Use XID index for direct lookup
INNER JOIN OBSHEAD H 
  USE INDEX (idx_OBSHEAD_GROUPID)  -- Filter on GROUPID first
  ON H.HDID = O.HDID
  AND H.GROUPID = 1300
INNER JOIN HIERGRPS HG 
  ON HG.GROUPID = H.GROUPID
INNER JOIN PATIENTPROFILE PT 
  USE INDEX (PRIMARY)  -- PID is primary key
  ON PT.PID = O.PID
INNER JOIN DOCUMENT D 
  USE INDEX (idx_DOCUMENT_SDID)
  ON D.SDID = O.SDID
INNER JOIN CONFTYPES CN 
  ON CN.CONFTYPEID = D.CONFTYPE
LEFT JOIN REL_OBS_EXT_CODE R1 
  USE INDEX (idx_REL_OBS_EXT_CODE_OBSID)
  ON O.OBSID = R1.OBSID 
  AND R1.EXT_CODE_ORDER = 1
LEFT JOIN EXT_CODE C1 
  USE INDEX (idx_EXT_CODE_ID)
  ON C1.EXT_CODE_ID = R1.EXT_CODE_ID
LEFT JOIN REL_OBS_EXT_CODE R2 
  USE INDEX (idx_REL_OBS_EXT_CODE_OBSID)
  ON O.OBSID = R2.OBSID 
  AND R2.EXT_CODE_ORDER = 2
LEFT JOIN EXT_CODE C2 
  USE INDEX (idx_EXT_CODE_ID)
  ON C2.EXT_CODE_ID = R2.EXT_CODE_ID
LEFT JOIN LABORDERPANEL LOP 
  USE INDEX (idx_LABORDERPANEL_ID)
  ON LOP.LABORDERPANELID = O.LABORDERPANELID
WHERE O.XID = 1E18

UNION ALL

-- ============================================================================
-- OPTIMIZED QUERY - BRANCH 2: CHANGE-based lookup with NOT EXISTS rewritten
-- ============================================================================
SELECT 
    O.OBSID,
    O.PID,
    O.XID,
    O.`CHANGE`,
    O.SDID,
    O.USRID,
    O.HDID,
    O.ABNORMAL,
    O.OBSDATE,
    O.OBSTYPE,
    O.OBSVALUE,
    O.PUBUSER,
    O.PUBTIME,
    O.PARENTID,
    O.`RANGE`,
    O.DESCRIPTION,
    CASE 
        WHEN O.STATE IS NULL OR O.STATE = '' THEN 'F'
        ELSE O.STATE
    END AS STATE,
    O.ENTRYID,
    O.ARCHIVE,
    O.DB_CREATE_DATE,
    O.DB_UPDATED_DATE,
    H.NAME,
    H.UNIT,
    H.DESCRIPTION AS OBSHEAD_DESCRIPTION,
    HG.GROUPNAME AS HG_GROUPNAME,
    HG.GROUPID AS HG_GROUPID,
    PT.SENSITIVECHART,
    C1.CODE AS C1_CODE,
    C1.CODING_SYSTEM_NAME AS C1_CODING_SYSTEM_NAME,
    C1.DESCRIPTION AS C1_DESCRIPTION,
    C2.CODE AS C2_CODE,
    C2.CODING_SYSTEM_NAME AS C2_CODING_SYSTEM_NAME,
    C2.DESCRIPTION AS C2_DESCRIPTION,
    CN.ABBR AS CONFABBR,
    IFNULL(CN.CONFTYPEID, 0) AS CONFTYPEID,
    LOP.CODE AS LOP_CODE,
    LOP.CODETYPE AS LOP_CODETYPE,
    LOP.NAME AS LOP_NAME,
    PT.LOCATIONID AS LOCATIONID
FROM OBS O
  USE INDEX (idx_OBS_CHANGE)  -- Use CHANGE index for second branch
INNER JOIN OBSHEAD H 
  USE INDEX (idx_OBSHEAD_GROUPID)  -- Filter on GROUPID first
  ON H.HDID = O.HDID
  AND H.GROUPID = 1300
INNER JOIN HIERGRPS HG 
  ON HG.GROUPID = H.GROUPID
INNER JOIN PATIENTPROFILE PT 
  USE INDEX (PRIMARY)  -- PID is primary key
  ON PT.PID = O.PID
INNER JOIN DOCUMENT D 
  USE INDEX (idx_DOCUMENT_SDID)
  ON D.SDID = O.SDID
INNER JOIN CONFTYPES CN 
  ON CN.CONFTYPEID = D.CONFTYPE
LEFT JOIN REL_OBS_EXT_CODE R1 
  USE INDEX (idx_REL_OBS_EXT_CODE_OBSID)
  ON O.OBSID = R1.OBSID 
  AND R1.EXT_CODE_ORDER = 1
LEFT JOIN EXT_CODE C1 
  USE INDEX (idx_EXT_CODE_ID)
  ON C1.EXT_CODE_ID = R1.EXT_CODE_ID
LEFT JOIN REL_OBS_EXT_CODE R2 
  USE INDEX (idx_REL_OBS_EXT_CODE_OBSID)
  ON O.OBSID = R2.OBSID 
  AND R2.EXT_CODE_ORDER = 2
LEFT JOIN EXT_CODE C2 
  USE INDEX (idx_EXT_CODE_ID)
  ON C2.EXT_CODE_ID = R2.EXT_CODE_ID
LEFT JOIN LABORDERPANEL LOP 
  USE INDEX (idx_LABORDERPANEL_ID)
  ON LOP.LABORDERPANELID = O.LABORDERPANELID
-- Converted NOT EXISTS to LEFT JOIN + NULL check
LEFT JOIN OBS O2
  USE INDEX (idx_OBS_change)
  ON O.PID = O2.PID 
  AND O.XID = O2.OBSID
WHERE O.`CHANGE` IN (0, 4, 10)
  AND O2.OBSID IS NULL  -- Replaces NOT EXISTS condition
;

-- ============================================================
-- SECTION 6: DEPLOYMENT INSTRUCTIONS
-- ============================================================
--
-- Step 1: CREATE INDEXES (in a maintenance window)
-- ============================================================
-- These indexes should be created on non-peak hours, as they will
-- lock the tables briefly (or block writes in older MySQL versions).
--
-- For very large tables (OBS with 33M rows, DOCUMENT with 20M rows),
-- use ALGORITHM=INPLACE, LOCK=NONE in MySQL 5.7.15+ or 8.0+:

-- ALTER TABLE OBS ADD INDEX idx_OBS_PID (PID), ALGORITHM=INPLACE, LOCK=NONE;
-- ALTER TABLE OBS ADD INDEX idx_OBS_XID (XID), ALGORITHM=INPLACE, LOCK=NONE;
-- ALTER TABLE OBS ADD INDEX idx_OBS_CHANGE (CHANGE, PID), ALGORITHM=INPLACE, LOCK=NONE;
-- ALTER TABLE OBSHEAD ADD INDEX idx_OBSHEAD_GROUPID (GROUPID, HDID), ALGORITHM=INPLACE, LOCK=NONE;
-- ALTER TABLE REL_OBS_EXT_CODE ADD INDEX idx_REL_OBS_EXT_CODE_OBSID (OBSID, EXT_CODE_ORDER), ALGORITHM=INPLACE, LOCK=NONE;
-- ALTER TABLE DOCUMENT ADD INDEX idx_DOCUMENT_SDID (SDID, CONFTYPE), ALGORITHM=INPLACE, LOCK=NONE;
-- ALTER TABLE EXT_CODE ADD INDEX idx_EXT_CODE_ID (EXT_CODE_ID), ALGORITHM=INPLACE, LOCK=NONE;
-- ALTER TABLE LABORDERPANEL ADD INDEX idx_LABORDERPANEL_ID (LABORDERPANELID), ALGORITHM=INPLACE, LOCK=NONE;

-- Step 2: Analyze table statistics
-- ============================================================
-- Run ANALYZE TABLE to update index statistics, so optimizer can make good decisions:

-- ANALYZE TABLE OBS;
-- ANALYZE TABLE OBSHEAD;
-- ANALYZE TABLE REL_OBS_EXT_CODE;
-- ANALYZE TABLE DOCUMENT;
-- ANALYZE TABLE EXT_CODE;
-- ANALYZE TABLE LABORDERPANEL;

-- Step 3: Test the optimized query in staging
-- ============================================================
-- Replace the original WHERE clause with the UNION ALL version.
-- Run EXPLAIN on both versions and compare:
--   - EXPLAIN FORMAT=JSON for detailed cost analysis
--   - Check actual execution time with EXPLAIN ANALYZE

-- Step 4: Monitor in production
-- ============================================================
-- After deployment:
--   - Check slow query log for any remaining slow queries
--   - Monitor index cardinality (SHOW INDEX FROM table_name)
--   - Run ANALYZE TABLE quarterly to keep statistics fresh
--

-- ============================================================
-- SECTION 7: PERFORMANCE EXPECTATIONS
-- ============================================================
--
-- BEFORE OPTIMIZATION:
--   - Full table scans: 33M + 20M + 10M + 10M + 1.3M rows = ~75M rows examined
--   - Hash join buffers: multiple (allocate RAM per join)
--   - Estimated time: 30-120 seconds (depending on cache)
--
-- AFTER OPTIMIZATION (with all indexes and UNION ALL):
--   - Rows examined: ~10K (XID branch) + ~200K (CHANGE branch) = ~210K
--   - Improvement factor: 75M / 210K = ~357x faster
--   - Estimated time: 0.1-0.5 seconds
--
-- Note: Actual improvement depends on:
--   - Current buffer pool hit rate
--   - Disk I/O performance
--   - Index selectivity of your data
--   - MySQL version and join algorithm (hash joins were slower before 8.0.20)
--

-- ============================================================
-- APPENDIX: SYNTAX NOTES
-- ============================================================
--
-- MySQL 8.0+: The UNION ALL with explicit column order should work as-is.
--
-- MySQL 5.7: Same syntax, but verify ALGORITHM=INPLACE support:
--   - Available in 5.7.5+ for most index operations
--   - Use online ALTER TABLE where possible
--
-- To verify index usage, run:
--   EXPLAIN FORMAT=JSON SELECT ...;
--   EXPLAIN (original query);
--   EXPLAIN (new UNION ALL query);
--
-- Look for:
--   - "type": "ref" or "eq_ref" (good, using index)
--   - "type": "range" (good for BETWEEN/IN)
--   - Avoid "type": "ALL" (full table scan)
--