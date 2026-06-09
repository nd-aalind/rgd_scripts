#!/usr/bin/env python3
"""
Optimized ETL: Build biogen.pasthistory_athenaone from 4 UDM source schemas.

Source schemas: udm_dcnd, udm_raleigh, udm_tncpa, udm_tng
Target table  : biogen.pasthistory_athenaone

Strategy:
  For each source schema, pre-materialize the 4 GROUP_CONCAT aggregations
  (surgical, social, medical, family) into staging tables ONCE.
  Then batch INSERT the final JOIN result by ndid from a pre-built all_keys staging.

  This avoids re-running expensive GROUP_CONCAT + 4-way LEFT JOIN per batch.

Optimizations applied:
- GROUP_CONCAT CTEs materialized once per source (not per batch)
- Per-source all_keys PK staging for batch boundary sampling
- Commit after every batch (frees undo/log space)
- Checkpoint/resume per source — re-run skips completed sources
- Disabled InnoDB checks per-session for bulk insert speed
- Progress bar via tqdm

Usage:
    python ao_biogen_past_hist_cons.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":            3306,
    "user":            "admin",
    "password":        "ClAx5UNkjnM8JgLG",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000
DEST_TABLE = "biogen_april.pasthistory_athenaone_may_full"

# Source schemas (label used for staging table names)
SOURCES = [
    {"label": "dcnd",    "schema": "udm_dcnd"},
    {"label": "raleigh", "schema": "udm_raleigh"},
    {"label": "tncpa",   "schema": "udm_tncpa"},
    {"label": "tng",     "schema": "udm_tng"},
]

BATCH_KEY = "ndid"

# ── Staging table names per source ────────────────────────────────────
def _stg(label, kind):
    return f"staging.biogen_ph_{kind}_{label}"

CHECKPOINT_TABLE = "staging.etl_checkpoint_biogen_past_hist_cons"


# ── Helpers ───────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


def _table_exists(cur, full_table_name):
    schema, table = full_table_name.split(".")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return cur.fetchone()[0] > 0


def _build_ranges(cur, staging_pk):
    """Compute batch boundary ranges from an all_keys staging table (by ndid)."""
    cur.execute(f"SELECT COUNT(DISTINCT {BATCH_KEY}) FROM {staging_pk}")
    total = cur.fetchone()[0]
    if total == 0:
        return [], 0

    cur.execute(f"""
        SELECT {BATCH_KEY}
        FROM (
            SELECT {BATCH_KEY},
                   ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
            FROM (SELECT DISTINCT {BATCH_KEY} FROM {staging_pk}) d
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {staging_pk}")
    max_pk = int(cur.fetchone()[0])

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    return ranges, total


# ── Checkpoint ────────────────────────────────────────────────────────

def is_done(conn, ck_key):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (ck_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, ck_key, status, rows=0, error=None):
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {CHECKPOINT_TABLE}
            (source_key, status, rows_inserted, started_at, completed_at, error_msg)
        VALUES (%s, %s, %s, NOW(), IF(%s = 'done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status        = VALUES(status),
            rows_inserted = VALUES(rows_inserted),
            completed_at  = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg     = VALUES(error_msg)
    """, (ck_key, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Staging materializers ──────────────────────────────────────────────

def materialize_source_stagings(cur, conn, src):
    """
    Pre-materialize the 4 GROUP_CONCAT CTEs + all_keys for one source schema.
    Skips each staging table if it already exists and has rows.
    """
    schema = src["schema"]
    label  = src["label"]

    stagings = [
        {
            "name":  _stg(label, "surgical"),
            "label": "surgical",
            "sql": f"""
                SELECT
                    TRIM(ndid) AS ndid,
                    COALESCE(encounter_date, surgery_date) AS encounter_date,
                    eid AS encounter_id,
                    GROUP_CONCAT(
                        CONCAT(COALESCE(surgery_name, ''), ' ', COALESCE(surgery_code, ''),
                               ' ^ ', COALESCE(surgery_reason, ''))
                        SEPARATOR ' + '
                    ) AS past_surgical_history
                FROM {schema}.surgical_history
                GROUP BY TRIM(ndid), COALESCE(encounter_date, surgery_date), eid
                HAVING TRIM(past_surgical_history) <> '^'
            """,
            "index": "ndid",
        },
        {
            "name":  _stg(label, "social"),
            "label": "social",
            "sql": f"""
                SELECT
                    ndid,
                    eid AS encounter_id,
                    COALESCE(encounter_date, social_hist_date) AS encounter_date,
                    GROUP_CONCAT(
                        CONCAT(COALESCE(social_category, ''), ' ^ ',
                               COALESCE(social_option, ''), ' ', COALESCE(social_notes, ''))
                        SEPARATOR ' + '
                    ) AS social_history_full
                FROM {schema}.social_history
                GROUP BY ndid, eid, COALESCE(encounter_date, social_hist_date)
                HAVING TRIM(social_history_full) <> '^'
            """,
            "index": "ndid",
        },
        {
            "name":  _stg(label, "medical"),
            "label": "medical",
            "sql": f"""
                SELECT
                    ndid,
                    eid AS encounter_id,
                    COALESCE(encounter_date, med_hist_date) AS encounter_date,
                    GROUP_CONCAT(
                        CONCAT(COALESCE(med_hist_question, ''), ' ^ ', COALESCE(med_hist_answer, ''))
                        SEPARATOR ' + '
                    ) AS medical_history
                FROM {schema}.medical_history
                GROUP BY ndid, eid, COALESCE(encounter_date, med_hist_date)
                HAVING TRIM(medical_history) <> '^'
            """,
            "index": "ndid",
        },
        {
            "name":  _stg(label, "family"),
            "label": "family",
            "sql": f"""
                SELECT
                    ndid,
                    eid AS encounter_id,
                    COALESCE(encounter_date, fam_hist_date) AS encounter_date,
                    GROUP_CONCAT(
                        CONCAT(COALESCE(fam_hist_relation, ''), ' ^ ', COALESCE(fam_hist_detail, ''))
                        SEPARATOR ' + '
                    ) AS family_history_notes
                FROM {schema}.family_history
                GROUP BY ndid, eid, COALESCE(encounter_date, fam_hist_date)
                HAVING TRIM(family_history_notes) <> '^'
            """,
            "index": "ndid",
        },
    ]

    for stg in stagings:
        tbl = stg["name"]
        print(f"    Materializing {stg['label']} staging for {label}...")
        needs_create = False
        if not _table_exists(cur, tbl):
            needs_create = True
        else:
            cur.execute(f"SELECT COUNT(*) FROM {tbl}")
            if cur.fetchone()[0] == 0:
                print(f"      found empty (prior failed run) — dropping...")
                cur.execute(f"DROP TABLE {tbl}")
                conn.commit()
                needs_create = True
            else:
                print(f"      already exists, reusing")

        if needs_create:
            cur.execute(f"CREATE TABLE {tbl} AS {stg['sql']}")
            cur.execute(f"ALTER TABLE {tbl} ADD INDEX idx_ndid ({stg['index']})")
            cur.execute(f"ALTER TABLE {tbl} ADD INDEX idx_ndid_date ({stg['index']}, encounter_date)")
            conn.commit()
            cur.execute(f"SELECT COUNT(*) FROM {tbl}")
            print(f"      {cur.fetchone()[0]:,} rows")

    # all_keys staging (distinct ndid + encounter_date from all 4 aggregations)
    all_keys_tbl = _stg(label, "allkeys")
    print(f"    Materializing all_keys staging for {label}...")
    needs_create = False
    if not _table_exists(cur, all_keys_tbl):
        needs_create = True
    else:
        cur.execute(f"SELECT COUNT(*) FROM {all_keys_tbl}")
        if cur.fetchone()[0] == 0:
            cur.execute(f"DROP TABLE {all_keys_tbl}")
            conn.commit()
            needs_create = True
        else:
            print(f"      already exists, reusing")

    if needs_create:
        surg = _stg(label, "surgical")
        soc  = _stg(label, "social")
        med  = _stg(label, "medical")
        fam  = _stg(label, "family")
        cur.execute(f"""
            CREATE TABLE {all_keys_tbl} AS
            SELECT ndid, encounter_date FROM {surg}
            UNION
            SELECT ndid, encounter_date FROM {soc}
            UNION
            SELECT ndid, encounter_date FROM {med}
            UNION
            SELECT ndid, encounter_date FROM {fam}
        """)
        cur.execute(f"ALTER TABLE {all_keys_tbl} ADD INDEX idx_ndid (ndid)")
        cur.execute(f"ALTER TABLE {all_keys_tbl} ADD INDEX idx_ndid_date (ndid, encounter_date)")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {all_keys_tbl}")
        print(f"      {cur.fetchone()[0]:,} rows")


# ── Batch INSERT builder ───────────────────────────────────────────────

def build_batch_insert(src, pk_lo, pk_hi):
    label  = src["label"]
    surg   = _stg(label, "surgical")
    soc    = _stg(label, "social")
    med    = _stg(label, "medical")
    fam    = _stg(label, "family")
    allkeys = _stg(label, "allkeys")

    return f"""
INSERT INTO {DEST_TABLE}
SELECT
    k.ndid,
    k.encounter_date,
    COALESCE(s.encounter_id, soc.encounter_id, m.encounter_id, f.encounter_id) AS encounter_id,
    m.medical_history,
    s.past_surgical_history,
    f.family_history_notes,
    soc.social_history_full
FROM {allkeys} k
LEFT JOIN {surg}  s   ON k.ndid = s.ndid   AND k.encounter_date = s.encounter_date
LEFT JOIN {soc}   soc ON k.ndid = soc.ndid AND k.encounter_date = soc.encounter_date
LEFT JOIN {med}   m   ON k.ndid = m.ndid   AND k.encounter_date = m.encounter_date
LEFT JOIN {fam}   f   ON k.ndid = f.ndid   AND k.encounter_date = f.encounter_date
WHERE k.{BATCH_KEY} >= {pk_lo}
  AND k.{BATCH_KEY} <  {pk_hi}
"""


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    """
    1. Drop and recreate destination table (explicit DDL).
    2. For each source, pre-materialize 4 GROUP_CONCAT stagings + all_keys staging.
    3. Create checkpoint table.
    4. Compute and return batch ranges per source.
    """
    conn = get_connection()
    cur  = conn.cursor()

    # Increase GROUP_CONCAT limit for this session (default 1024 is too small)
    cur.execute("SET SESSION group_concat_max_len = 1073741824")  # 1 GB

    # ── 1. Destination table ─────────────────────────────────────────
    print(f"  Recreating {DEST_TABLE}...")
    cur.execute(f"DROP TABLE IF EXISTS {DEST_TABLE}")
    cur.execute(f"""
        CREATE TABLE {DEST_TABLE} (
            ndid                 BIGINT,
            encounter_date       DATE,
            encounter_id         BIGINT,
            medical_history      LONGTEXT,
            past_surgical_history LONGTEXT,
            family_history_notes LONGTEXT,
            social_history_full  LONGTEXT
        ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC
    """)
    conn.commit()
    print(f"    created (empty)")

    # ── 2. Per-source staging tables ─────────────────────────────────
    for src in SOURCES:
        print(f"\n  Preparing stagings for source: {src['label']} ({src['schema']})...")
        materialize_source_stagings(cur, conn, src)

    # ── 3. Checkpoint table ──────────────────────────────────────────
    print("\n  Creating checkpoint table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key    VARCHAR(150) NOT NULL PRIMARY KEY,
            status        ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_inserted BIGINT      DEFAULT 0,
            started_at    DATETIME    DEFAULT NULL,
            completed_at  DATETIME    DEFAULT NULL,
            error_msg     TEXT        DEFAULT NULL
        )
    """)
    conn.commit()
    print("    ready")

    # ── 4. Compute batch ranges per source ───────────────────────────
    all_ranges = {}
    for src in SOURCES:
        allkeys_tbl = _stg(src["label"], "allkeys")
        ranges, total = _build_ranges(cur, allkeys_tbl)
        all_ranges[src["label"]] = ranges
        print(f"    {src['label']}: {total:,} (ndid, date) rows → {len(ranges)} batches")

    cur.close()
    conn.close()
    return all_ranges


# ── Runner ─────────────────────────────────────────────────────────────

def run_source(src, ranges, pbar):
    label  = src["label"]
    ck_key = f"biogen_past_hist.{label}"

    conn = get_connection()

    if is_done(conn, ck_key):
        conn.close()
        pbar.update(len(ranges))
        return {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, ck_key, "running")
    t0 = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_batch_insert(src, pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "done", total_rows)
        conn.close()
        return {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Biogen Past History Consolidation — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  sources    : {', '.join(s['schema'] for s in SOURCES)}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Setting up tables...", flush=True)
    all_ranges = setup_tables()

    results = {}
    any_failed = False

    total_batches = sum(len(all_ranges.get(src["label"], [])) for src in SOURCES)
    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        for src in SOURCES:
            label  = src["label"]
            ranges = all_ranges.get(label, [])

            if not ranges:
                print(f"\n  [SKIP] {label} — no eligible rows")
                continue

            print(f"\n  Starting {label} ({len(ranges)} batches)...")
            result = run_source(src, ranges, pbar)
            results[label] = result

            if result["status"].startswith("FAILED"):
                print(f"\n  FAILED at {label}: {result['status']}")
                print("  Aborting remaining sources.")
                any_failed = True
                break

    print(f"\n{'='*70}")
    print(f"  Per-source summary:")
    total_rows = 0
    for src in SOURCES:
        label = src["label"]
        res = results.get(label, {"status": "not run", "rows": 0, "secs": 0})
        status = res["status"]
        rows   = res["rows"]
        secs   = res["secs"]
        if status == "done":
            tag = " DONE"; total_rows += rows
        elif status == "skipped":
            tag = " SKIP"
        elif status == "not run":
            tag = "  ---"
        else:
            tag = " FAIL"; any_failed = True
        print(f"  [{tag}] {label:<10}  {rows:>10,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    print(f"\n  Total rows inserted : {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    for src in SOURCES:
        lbl = src["label"]
        for kind in ["surgical", "social", "medical", "family", "allkeys"]:
            print(f"    DROP TABLE IF EXISTS {_stg(lbl, kind)};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
