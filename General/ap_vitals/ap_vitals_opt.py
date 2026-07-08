#!/usr/bin/env python3
"""
ap_vitals_opt.py — AthenaPlus vitals ETL (optimized)

Loads vitals from AthenaPlus OBS into the destination table in parallel batches.

Sources (2 independent INSERT workers, split from original OR condition):
  1. xid    — WHERE O.XID = 1E18  (direct lookup via idx_OBS_XID)
  2. change — WHERE O.CHANGE IN (0,4,10) AND no parent (LEFT JOIN + NULL check)

Optimizations:
- OR condition split into UNION ALL — each branch uses its own index
- NOT EXISTS rewritten as LEFT JOIN + NULL check in staging PK creation
- DOCUMENT + CONFTYPES pre-materialized into staging (out of batch hot path)
- Batching by actual OBSID values from each branch's staging PK (sparse-ID safe)
- Two parallel workers (one per branch)
- Checkpoint/resume per branch — re-run skips completed branches
- Commit after every batch
- InnoDB checks disabled per-session for bulk speed
- tqdm progress bar
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pymysql
from tqdm import tqdm
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ─────────────────────────────────────────────────────
SOURCE_SCHEMA = "noran"   # ← change per run
PSID          = 7         # ← change per run

DB_CONFIG = {
    "host":            os.environ.get("DB_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_USER"),
    "password":        os.environ.get("DB_PASSWORD"),
    "database":        SOURCE_SCHEMA,
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

DEST_TABLE       = "rgd_udm_staging.ap_vitals"
STAGING_DOC      = f"staging.ap_vitals_doc_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_ap_vitals_{SOURCE_SCHEMA}"

BATCH_SIZE  = 50_000
MAX_WORKERS = 2
BATCH_KEY   = "OBSID"

SOURCES = [
    {
        "key":        f"ap_vitals_xid_{SOURCE_SCHEMA}",
        "branch":     "xid",
        "pk_staging": f"staging.ap_vitals_pk_xid_{SOURCE_SCHEMA}",
    },
    {
        "key":        f"ap_vitals_chg_{SOURCE_SCHEMA}",
        "branch":     "change",
        "pk_staging": f"staging.ap_vitals_pk_chg_{SOURCE_SCHEMA}",
    },
]


# ── Helpers ───────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


def _table_exists(cur, full_table_name: str) -> bool:
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return cur.fetchone()[0] > 0


def _named_index_exists(cur, schema: str, table: str, index_name: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND index_name = %s",
        (schema, table, index_name),
    )
    return cur.fetchone()[0] > 0


def _build_ranges(cur, staging_pk: str):
    cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
    total = cur.fetchone()[0]
    if total == 0:
        return [], 0

    cur.execute(f"""
        SELECT {BATCH_KEY}
        FROM (
            SELECT {BATCH_KEY},
                   ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
            FROM {staging_pk}
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

def is_done(conn, source_key: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (source_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, source_key: str, status: str, rows: int = 0, error: str = None) -> None:
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
    """, (source_key, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Batch INSERT builder ───────────────────────────────────────────────
# Both branches share the same SELECT — the filter is already baked into
# each branch's staging PK, so the batch INSERT just drives from that.

def build_batch_insert(src: dict, pk_lo, pk_hi) -> str:
    staging_pk = src["pk_staging"]
    return f"""
INSERT INTO {DEST_TABLE}
    (OBSID, PID, XID, `CHANGE`, SDID, USRID, HDID,
     ABNORMAL, OBSDATE, OBSTYPE, OBSVALUE, PUBUSER, PUBTIME, PARENTID,
     `RANGE`, DESCRIPTION, STATE, ENTRYID, ARCHIVE,
     DB_CREATE_DATE, DB_UPDATED_DATE,
     NAME, UNIT, OBSHEAD_DESCRIPTION,
     HG_GROUPNAME, HG_GROUPID,
     SENSITIVECHART,
     C1_CODE, C1_CODING_SYSTEM_NAME, C1_DESCRIPTION,
     C2_CODE, C2_CODING_SYSTEM_NAME, C2_DESCRIPTION,
     CONFABBR, CONFTYPEID,
     LOP_CODE, LOP_CODETYPE, LOP_NAME,
     LOCATIONID)
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
    CASE WHEN O.STATE IS NULL OR O.STATE = '' THEN 'F' ELSE O.STATE END,
    O.ENTRYID,
    O.ARCHIVE,
    O.DB_CREATE_DATE,
    O.DB_UPDATED_DATE,
    H.NAME,
    H.UNIT,
    H.DESCRIPTION,
    HG.GROUPNAME,
    HG.GROUPID,
    PT.SENSITIVECHART,
    C1.CODE,
    C1.CODING_SYSTEM_NAME,
    C1.DESCRIPTION,
    C2.CODE,
    C2.CODING_SYSTEM_NAME,
    C2.DESCRIPTION,
    D.CONFABBR,
    D.CONFTYPEID,
    LOP.CODE,
    LOP.CODETYPE,
    LOP.NAME,
    PT.LOCATIONID
FROM {staging_pk} pk
INNER JOIN {SOURCE_SCHEMA}.OBS O
    ON  O.OBSID = pk.OBSID
INNER JOIN {SOURCE_SCHEMA}.PatientProfile PT
    ON  PT.PID = O.PID
INNER JOIN {STAGING_DOC} D
    ON  D.SDID = O.SDID
INNER JOIN {SOURCE_SCHEMA}.OBSHEAD H USE INDEX (idx_OBSHEAD_GROUPID)
    ON  H.HDID = O.HDID
INNER JOIN {SOURCE_SCHEMA}.HIERGRPS HG
    ON  HG.GROUPID = H.GROUPID
LEFT JOIN {SOURCE_SCHEMA}.REL_OBS_EXT_CODE R1 USE INDEX (idx_REL_OBS_EXT_CODE_OBSID)
    ON  R1.OBSID = O.OBSID AND R1.EXT_CODE_ORDER = 1
LEFT JOIN {SOURCE_SCHEMA}.REL_OBS_EXT_CODE R2 USE INDEX (idx_REL_OBS_EXT_CODE_OBSID)
    ON  R2.OBSID = O.OBSID AND R2.EXT_CODE_ORDER = 2
LEFT JOIN {SOURCE_SCHEMA}.EXT_CODE C1 USE INDEX (idx_EXT_CODE_ID)
    ON  C1.EXT_CODE_ID = R1.EXT_CODE_ID
LEFT JOIN {SOURCE_SCHEMA}.EXT_CODE C2 USE INDEX (idx_EXT_CODE_ID)
    ON  C2.EXT_CODE_ID = R2.EXT_CODE_ID
LEFT JOIN {SOURCE_SCHEMA}.LABORDERPANEL LOP USE INDEX (idx_LABORDERPANEL_ID)
    ON  LOP.LABORDERPANELID = O.LABORDERPANELID
WHERE pk.OBSID >= {pk_lo}
  AND pk.OBSID <  {pk_hi}
"""


# ── Setup ─────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    # Create named indexes matched to USE INDEX hints
    named_indexes = [
        ("OBS",                  "idx_OBS_XID",                  "XID"),
        ("OBS",                  "idx_OBS_CHANGE",               "`CHANGE`, PID"),
        ("OBS",                  "idx_OBS_PID",                  "PID"),
        ("OBS",                  "idx_OBS_SDID",                 "SDID"),
        ("OBS",                  "idx_OBS_HDID",                 "HDID"),
        ("OBS",                  "idx_OBS_LABORDERPANELID",      "LABORDERPANELID"),
        ("OBSHEAD",              "idx_OBSHEAD_GROUPID",          "GROUPID, HDID"),
        ("REL_OBS_EXT_CODE",     "idx_REL_OBS_EXT_CODE_OBSID",  "OBSID, EXT_CODE_ORDER"),
        ("EXT_CODE",             "idx_EXT_CODE_ID",              "EXT_CODE_ID"),
        ("LABORDERPANEL",        "idx_LABORDERPANEL_ID",         "LABORDERPANELID"),
        ("DOCUMENT",             "idx_DOCUMENT_SDID",            "SDID"),
        ("HIERGRPS",             "idx_HIERGRPS_GROUPID",         "GROUPID"),
    ]
    for tbl, idx_name, cols in named_indexes:
        print(f"    Checking {idx_name} on {SOURCE_SCHEMA}.{tbl}...", end=" ", flush=True)
        if not _named_index_exists(cur, SOURCE_SCHEMA, tbl, idx_name):
            print(f"missing — creating...", flush=True)
            cur.execute(f"CREATE INDEX {idx_name} ON {SOURCE_SCHEMA}.{tbl} ({cols})")
            conn.commit()
            print(f"      done")
        else:
            print("exists")

    # Materialize DOCUMENT + CONFTYPES — eliminates two-table join per batch
    print(f"  Materializing {STAGING_DOC}...")
    if not _table_exists(cur, STAGING_DOC):
        cur.execute(f"""
            CREATE TABLE {STAGING_DOC} AS
            SELECT
                D.SDID,
                CN.ABBR                   AS CONFABBR,
                IFNULL(CN.CONFTYPEID, 0)  AS CONFTYPEID
            FROM {SOURCE_SCHEMA}.DOCUMENT D USE INDEX (idx_DOCUMENT_SDID)
            INNER JOIN {SOURCE_SCHEMA}.CONFTYPES CN
                ON CN.CONFTYPEID = D.CONFTYPE
        """)
        cur.execute(f"ALTER TABLE {STAGING_DOC} ADD INDEX idx_sdid (SDID)")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_DOC}")
        n = cur.fetchone()[0]
        print(f"    {n:,} document rows materialized")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_DOC}")
        n = cur.fetchone()[0]
        print(f"    already exists, reusing  ({n:,} rows)")

    # Create destination table
    print(f"  Creating destination table {DEST_TABLE} if needed...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            OBSID                   BIGINT        DEFAULT NULL,
            PID                     BIGINT        DEFAULT NULL,
            XID                     BIGINT        DEFAULT NULL,
            `CHANGE`                INT           DEFAULT NULL,
            SDID                    BIGINT        DEFAULT NULL,
            USRID                   BIGINT        DEFAULT NULL,
            HDID                    BIGINT        DEFAULT NULL,
            ABNORMAL                VARCHAR(10)   DEFAULT NULL,
            OBSDATE                 DATETIME      DEFAULT NULL,
            OBSTYPE                 VARCHAR(50)   DEFAULT NULL,
            OBSVALUE                TEXT          DEFAULT NULL,
            PUBUSER                 VARCHAR(100)  DEFAULT NULL,
            PUBTIME                 DATETIME      DEFAULT NULL,
            PARENTID                BIGINT        DEFAULT NULL,
            `RANGE`                 VARCHAR(255)  DEFAULT NULL,
            DESCRIPTION             TEXT          DEFAULT NULL,
            STATE                   VARCHAR(10)   DEFAULT NULL,
            ENTRYID                 BIGINT        DEFAULT NULL,
            ARCHIVE                 TINYINT       DEFAULT NULL,
            DB_CREATE_DATE          DATETIME      DEFAULT NULL,
            DB_UPDATED_DATE         DATETIME      DEFAULT NULL,
            NAME                    VARCHAR(255)  DEFAULT NULL,
            UNIT                    VARCHAR(100)  DEFAULT NULL,
            OBSHEAD_DESCRIPTION     TEXT          DEFAULT NULL,
            HG_GROUPNAME            VARCHAR(255)  DEFAULT NULL,
            HG_GROUPID              INT           DEFAULT NULL,
            SENSITIVECHART          VARCHAR(10)   DEFAULT NULL,
            C1_CODE                 VARCHAR(255)  DEFAULT NULL,
            C1_CODING_SYSTEM_NAME   VARCHAR(255)  DEFAULT NULL,
            C1_DESCRIPTION          TEXT          DEFAULT NULL,
            C2_CODE                 VARCHAR(255)  DEFAULT NULL,
            C2_CODING_SYSTEM_NAME   VARCHAR(255)  DEFAULT NULL,
            C2_DESCRIPTION          TEXT          DEFAULT NULL,
            CONFABBR                VARCHAR(50)   DEFAULT NULL,
            CONFTYPEID              INT           DEFAULT NULL,
            LOP_CODE                VARCHAR(255)  DEFAULT NULL,
            LOP_CODETYPE            VARCHAR(50)   DEFAULT NULL,
            LOP_NAME                VARCHAR(255)  DEFAULT NULL,
            LOCATIONID              BIGINT        DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # Checkpoint table
    print("  Creating checkpoint table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key    VARCHAR(200) NOT NULL PRIMARY KEY,
            status        ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_inserted BIGINT      DEFAULT 0,
            started_at    DATETIME    DEFAULT NULL,
            completed_at  DATETIME    DEFAULT NULL,
            error_msg     TEXT        DEFAULT NULL
        )
    """)
    conn.commit()
    print("    ready")

    # Build staging PK tables — one per branch
    source_ranges = {}
    for src in SOURCES:
        key        = src["key"]
        branch     = src["branch"]
        staging_pk = src["pk_staging"]

        print(f"  Creating staging PK table {staging_pk} ({branch} branch)...")
        if not _table_exists(cur, staging_pk):
            if branch == "xid":
                # Branch 1: direct XID lookup
                cur.execute(f"""
                    CREATE TABLE {staging_pk} AS
                    SELECT O.OBSID
                    FROM {SOURCE_SCHEMA}.OBS O USE INDEX (idx_OBS_XID)
                    INNER JOIN {SOURCE_SCHEMA}.OBSHEAD H USE INDEX (idx_OBSHEAD_GROUPID)
                        ON H.HDID = O.HDID AND H.GROUPID = 1300
                    WHERE O.XID = 1E18
                """)
            else:
                # Branch 2: CHANGE filter, NOT EXISTS rewritten as LEFT JOIN + NULL
                cur.execute(f"""
                    CREATE TABLE {staging_pk} AS
                    SELECT O.OBSID
                    FROM {SOURCE_SCHEMA}.OBS O USE INDEX (idx_OBS_CHANGE)
                    INNER JOIN {SOURCE_SCHEMA}.OBSHEAD H USE INDEX (idx_OBSHEAD_GROUPID)
                        ON H.HDID = O.HDID AND H.GROUPID = 1300
                    LEFT JOIN {SOURCE_SCHEMA}.OBS O2 USE INDEX (idx_OBS_XID)
                        ON O2.PID = O.PID AND O2.OBSID = O.XID
                    WHERE O.`CHANGE` IN (0, 4, 10)
                      AND O2.OBSID IS NULL
                """)

            cur.execute(f"ALTER TABLE {staging_pk} ADD INDEX idx_pk ({BATCH_KEY})")
            conn.commit()
            cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
            n = cur.fetchone()[0]
            print(f"    {n:,} qualifying OBSIDs")
        else:
            cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
            n = cur.fetchone()[0]
            print(f"    already exists, reusing  ({n:,} rows)")

        ranges, total = _build_ranges(cur, staging_pk)
        print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,}  (total: {total:,})")
        source_ranges[key] = ranges

    cur.close()
    conn.close()
    return source_ranges


# ── Worker ────────────────────────────────────────────────────────────

def run_source(src: dict, ranges: list, pbar) -> dict:
    key  = src["key"]
    conn = get_connection()
    t0   = time.time()
    total_rows = 0

    if is_done(conn, key):
        conn.close()
        pbar.update(len(ranges))
        return {"source": key, "status": "skipped", "rows": 0, "secs": 0.0}

    mark(conn, key, "running")

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for lo, hi in ranges:
            cur.execute(build_batch_insert(src, lo, hi))
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, key, "done", total_rows)
        conn.close()
        return {"source": key, "status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        err_msg = str(exc)
        print(f"\n  [ERROR] {key}: {err_msg}")
        try:
            mark(conn, key, "failed", total_rows, err_msg)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        return {"source": key, "status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  AthenaPlus Vitals ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source      : {SOURCE_SCHEMA}  (psid={PSID})")
    print(f"  dest        : {DEST_TABLE}")
    print(f"  staging doc : {STAGING_DOC}")
    print(f"  checkpoint  : {CHECKPOINT_TABLE}")
    print(f"  workers     : {MAX_WORKERS}  |  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    source_ranges = setup_tables()

    total_batches = sum(len(r) for r in source_ranges.values())
    if total_batches == 0:
        print("  No eligible rows found. Exiting.")
        return

    results = []
    with tqdm(total=total_batches, desc="ap_vitals", unit="batch") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(run_source, src, source_ranges[src["key"]], pbar): src
                for src in SOURCES
                if source_ranges.get(src["key"])
            }
            for future in as_completed(futures):
                results.append(future.result())

    print()
    for r in sorted(results, key=lambda x: x["source"]):
        tag = "DONE" if r["status"] == "done" \
              else "SKIP" if r["status"] == "skipped" \
              else "FAIL"
        print(f"  [{tag}] {r['source']:<45} {r['rows']:>10,} rows  ({r['secs']}s)")

    done    = sum(1 for r in results if r["status"] == "done")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = [r for r in results if "FAILED" in str(r["status"])]
    total   = sum(r["rows"] for r in results)

    print(f"\n{'='*70}")
    print(f"  Done: {done}  Skipped: {skipped}  Failed: {len(failed)}  |  Total rows: {total:,}")
    print(f"{'='*70}")

    if failed:
        print("\n  Failed sources:")
        for r in failed:
            print(f"    {r['source']}: {r['status']}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_DOC};")
    for src in SOURCES:
        print(f"    DROP TABLE IF EXISTS {src['pk_staging']};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    print()

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
