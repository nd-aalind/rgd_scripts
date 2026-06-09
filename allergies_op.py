#!/usr/bin/env python3
"""
Optimized allergen standardisation UPDATE for: kinsula_leq.allergies

Joins to three external schemas to standardise allergen codes:
  - semantics.snomed             (SNOMED CT — matched by allergen_code = conceptId)
  - FDB.RNDC14_NDC_MSTR          (NDC      — matched by REPLACE/TRIM allergen_code = NDC)
  - FDB.REVDCS0_RXN_CONCEPT_SOURCE + FDB.REVDCD0_RXN_CONCEPT_DESC
                                 (RXNORM   — matched by allergen_code = EVD_RXN_RXCUI)

Updates three columns:
  - allergen_name_std            (COALESCE: rxdesc name > NDC brand name > SNOMED term)
  - allergen_code_std            (COALESCE: RXCUI > NDC > SNOMED conceptId)
  - allergen_coding_system_std   (RXNORM / NDC / SNOMED / NS)

Filter: allergen_code IS NOT NULL AND allergen_code <> ''

Optimizations applied:
- CTEs (snomed_max, rx_max) pre-materialized as indexed staging tables — computed
  ONCE instead of being re-executed on every batch
- Batch key staging pre-filters to eligible allergen_code rows
- Batch by actual primary key values (not arithmetic ranges — IDs can be sparse)
- Server-side boundary sampling (avoids loading millions of PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python allergies_op.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "172.16.2.42",
    "port":            3306,
    "user":            "nd-root-mysql",
    "password":        "kmsamd89undsd4",
    "database":        "kinsula_leq",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 10_000   # rows per batch (5 JOINs across 3 schemas — keep batches small)

TARGET_TABLE      = "kinsula_leq.allergies"
STAGING_PK        = "staging.tmp_allergies_pk_staging"
STAGING_SNOMED    = "staging.tmp_allergies_snomed_max"
STAGING_RX        = "staging.tmp_allergies_rx_max"
CHECKPOINT_TABLE  = "staging.etl_checkpoint_allergies_std"

# ── Primary key column used for batching ─────────────────────────────
BATCH_KEY = "udm_inc_id"

CHECKPOINT_KEY = "allergies.allergen_std_update"


# ── Helpers ──────────────────────────────────────────────────────────

def get_connection():
    """One connection per call."""
    return pymysql.connect(**DB_CONFIG)


def build_batch_update(pk_lo, pk_hi):
    """
    Reproduces the original UPDATE logic in batched form.
    The CTEs (snomed_max, rx_max) are replaced with pre-materialized staging
    tables so they are not re-computed on every batch.
    Original JOIN chain and SET expressions are preserved exactly.
    """
    return f"""
UPDATE {TARGET_TABLE} a
LEFT JOIN {STAGING_SNOMED} sm
    ON a.allergen_code = sm.conceptId
LEFT JOIN semantics.snomed s
    ON sm.conceptId = s.conceptId
   AND s.Id = sm.latest_snomed_id
LEFT JOIN FDB.RNDC14_NDC_MSTR ndc
    ON REPLACE(TRIM(a.allergen_code), '_', '') = ndc.NDC
LEFT JOIN {STAGING_RX} rxm
    ON rxm.EVD_RXN_RXCUI = TRIM(a.allergen_code)
LEFT JOIN FDB.REVDCS0_RXN_CONCEPT_SOURCE rx
    ON rx.EVD_RXN_RXCUI = TRIM(a.allergen_code)
   AND rx.EVD_RXN_CONCEPT_SOURCE_KEY = rxm.latest_rx_id
LEFT JOIN FDB.REVDCD0_RXN_CONCEPT_DESC rxdesc
    ON rx.EVD_RXN_CONCEPT_SOURCE_KEY = rxdesc.EVD_RXN_CONCEPT_SOURCE_KEY
SET
    a.allergen_name_std = COALESCE(rxdesc.EVD_RXN_STR, ndc.BN, s.term, 'NS'),
    a.allergen_code_std = COALESCE(rx.EVD_RXN_RXCUI, ndc.NDC, s.conceptId, 'NS'),
    a.allergen_coding_system_std = CASE
        WHEN rx.EVD_RXN_RXCUI IS NOT NULL
             AND ndc.NDC IS NULL
             AND s.conceptId IS NULL THEN 'RXNORM'
        WHEN ndc.NDC IS NOT NULL
             AND rx.EVD_RXN_RXCUI IS NULL
             AND s.conceptId IS NULL THEN 'NDC'
        WHEN s.conceptId IS NOT NULL
             AND rx.EVD_RXN_RXCUI IS NULL
             AND ndc.NDC IS NULL THEN 'SNOMED'
        ELSE 'NS'
    END
WHERE a.allergen_code IS NOT NULL
  AND a.allergen_code <> ''
  AND a.{BATCH_KEY} >= {pk_lo} AND a.{BATCH_KEY} < {pk_hi}
"""


# ── Checkpoint ───────────────────────────────────────────────────────

def is_done(conn):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (CHECKPOINT_KEY,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, status, rows=0, error=None):
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {CHECKPOINT_TABLE}
            (source_key, status, rows_updated, started_at, completed_at, error_msg)
        VALUES (%s, %s, %s, NOW(), IF(%s = 'done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status       = VALUES(status),
            rows_updated = VALUES(rows_updated),
            completed_at = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg    = VALUES(error_msg)
    """, (CHECKPOINT_KEY, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Setup ────────────────────────────────────────────────────────────

def _table_exists(cur, full_table_name):
    schema, table = full_table_name.split(".")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return cur.fetchone()[0] > 0


def _index_exists(cur, full_table_name, index_name):
    schema, table = full_table_name.split(".")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND index_name = %s",
        (schema, table, index_name),
    )
    return cur.fetchone()[0] > 0


def setup_tables():
    """Materialize CTEs + create PK staging + checkpoint. Return pk ranges."""
    conn = get_connection()
    cur = conn.cursor()

    # ── 1. Pre-materialize snomed_max CTE ────────────────────────────
    print("  Materializing snomed_max (semantics.snomed active conceptIds)...")
    if not _table_exists(cur, STAGING_SNOMED):
        cur.execute(f"""
            CREATE TABLE {STAGING_SNOMED} AS
            SELECT
                conceptId,
                MAX(id) AS latest_snomed_id
            FROM semantics.snomed
            WHERE active = 1
            GROUP BY conceptId
        """)
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    if not _index_exists(cur, STAGING_SNOMED, "idx_concept"):
        cur.execute(f"ALTER TABLE {STAGING_SNOMED} ADD INDEX idx_concept (conceptId(191))")
        conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_SNOMED}")
    print(f"    {cur.fetchone()[0]:,} SNOMED concept rows")

    # ── 2. Pre-materialize rx_max CTE ────────────────────────────────
    print("  Materializing rx_max (FDB.REVDCS0_RXN_CONCEPT_SOURCE)...")
    if not _table_exists(cur, STAGING_RX):
        cur.execute(f"""
            CREATE TABLE {STAGING_RX} AS
            SELECT
                EVD_RXN_RXCUI,
                MAX(EVD_RXN_CONCEPT_SOURCE_KEY) AS latest_rx_id
            FROM FDB.REVDCS0_RXN_CONCEPT_SOURCE
            GROUP BY EVD_RXN_RXCUI
        """)
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    if not _index_exists(cur, STAGING_RX, "idx_rxcui"):
        cur.execute(f"ALTER TABLE {STAGING_RX} ADD INDEX idx_rxcui (EVD_RXN_RXCUI)")
        conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_RX}")
    print(f"    {cur.fetchone()[0]:,} RxNorm concept rows")

    # ── 3. PK staging table: pre-filter eligible allergen rows ───────
    print("  Creating PK staging table (non-null, non-empty allergen_code rows)...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
              AND allergen_code IS NOT NULL
              AND allergen_code <> ''
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
    row_count = cur.fetchone()[0]
    print(f"    {row_count:,} eligible rows to update")

    # ── 4. Checkpoint table ──────────────────────────────────────────
    print("  Creating checkpoint table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key   VARCHAR(150) NOT NULL PRIMARY KEY,
            status       ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_updated BIGINT      DEFAULT 0,
            started_at   DATETIME    DEFAULT NULL,
            completed_at DATETIME    DEFAULT NULL,
            error_msg    TEXT        DEFAULT NULL
        )
    """)
    conn.commit()
    print("    ready")

    # ── 5. Compute batch ranges via server-side boundary sampling ────
    print("  Computing batch boundaries...")
    sys.stdout.flush()

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
    total = cur.fetchone()[0]

    if total == 0:
        cur.close()
        conn.close()
        return []

    cur.execute(f"""
        SELECT {BATCH_KEY}
        FROM (
            SELECT {BATCH_KEY},
                   ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
            FROM {STAGING_PK}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {STAGING_PK}")
    max_pk = int(cur.fetchone()[0])

    cur.close()
    conn.close()

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")
    return ranges


# ── Runner ───────────────────────────────────────────────────────────

def run_update(ranges, pbar):
    """Execute the allergen standardisation UPDATE across all batches."""
    conn = get_connection()

    if is_done(conn):
        conn.close()
        pbar.update(len(ranges))
        return {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, "running")
    t0 = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()

        # Disable InnoDB checks for bulk update speed (session-scoped only)
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_batch_update(pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        # Re-enable checks
        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, "done", total_rows)
        conn.close()
        return {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ─────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Allergen Standardisation UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  lookup 1   : semantics.snomed       (SNOMED — allergen_code = conceptId)")
    print(f"  lookup 2   : FDB.RNDC14_NDC_MSTR    (NDC    — allergen_code = NDC)")
    print(f"  lookup 3   : FDB.REVDCS0/REVDCD0    (RXNORM — allergen_code = RXCUI)")
    print(f"  filter     : allergen_code IS NOT NULL AND allergen_code <> ''")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  staging pk : {STAGING_PK}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()
    if not ranges:
        print(f"\nNo eligible rows found in {TARGET_TABLE}. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="Overall", unit="batch") as pbar:
        result = run_update(ranges, pbar)

    print()
    if result["status"] == "done":
        tag = " DONE"
    elif result["status"] == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"

    print(f"  [{tag}] {TARGET_TABLE:<42} {result['rows']:>10,} rows updated  ({result['secs']}s)")

    print(f"\n{'='*70}")
    if result["status"].startswith("FAILED"):
        print(f"  FAILED: {result['status']}")
    else:
        print(f"  Status: {result['status']}  |  Total rows updated: {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {STAGING_SNOMED};")
    print(f"    DROP TABLE IF EXISTS {STAGING_RX};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
