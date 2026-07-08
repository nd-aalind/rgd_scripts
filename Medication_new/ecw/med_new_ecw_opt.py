#!/usr/bin/env python3
"""
Optimized ETL for: suven.medication
Source: eClinicalWorks (eCW) — fcn_latest schema

Two phases before batching:
  1. Materialize oldrxdetail_pivot (properties pivoted per oldrxid) — computed once
  2. Batched INSERT from oldrxmain → suven.medication

Pre-materialized lookup:
  staging.med_ecw_pivot_v1_{schema}   (oldrxdetail GROUP BY oldrxid — pivoted properties)

Optimizations:
- oldrxdetail pivot materialized once (avoids re-running GROUP BY per batch)
- Batching by actual oldrxmain.oldrxid PK values (sparse-ID safe)
- Checkpoint/resume — re-run skips completed sources
- InnoDB checks disabled per-session for bulk speed
- Commit after every batch
- tqdm progress bar

Usage:
    python med_new_ecw_opt.py
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

# ── Configuration ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_USER"),
    "password":        os.environ.get("DB_PASSWORD"),
    "database":        "fcn_latest",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000
MAX_WORKERS = 1

# ── Change this variable to run for a different schema/psid ───────────────────
SOURCE_SCHEMA = "fcn_latest"
PSID          = 8

DEST_TABLE       = "suven.medication"
STAGING_PIVOT    = f"staging.med_ecw_pivot_v1_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_med_ecw_v1_{SOURCE_SCHEMA}"

SOURCES = [
    {
        "key":        "oldrxmain",
        "table":      "oldrxmain",
        "pk":         "oldrxid",
        "pk_staging": f"staging.tmp_med_ecw_pk_v1_{SOURCE_SCHEMA}",
    },
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


def _table_exists(cur, full_table_name):
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return cur.fetchone()[0] > 0


def _index_exists(cur, schema, table, column):
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, column),
    )
    return cur.fetchone()[0] > 0


# ── Checkpoint ────────────────────────────────────────────────────────────────

def is_done(conn, source_key):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (source_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, source_key, status, rows=0, error=None):
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


# ── Batch INSERT builder ──────────────────────────────────────────────────────

# Reusable date-safe expressions for startdate / stopdate (VARCHAR in some instances)
_START = (
    "CASE WHEN CAST(b.startdate AS CHAR) = 'None' THEN NULL"
    " WHEN YEAR(b.startdate) < 1991 THEN NULL"
    " ELSE DATE(LEFT(b.startdate, 10)) END"
)
_STOP = (
    "CASE WHEN CAST(b.stopdate AS CHAR) = 'None' THEN NULL"
    " WHEN YEAR(b.stopdate) < 1991 THEN NULL"
    " ELSE DATE(LEFT(b.stopdate, 10)) END"
)


def build_batch_insert(pk_lo, pk_hi):
    return f"""
INSERT INTO {DEST_TABLE}
    (source, med_id, ndid, enc_date, eid,
     written_date, med_administered_datetime, doc_orderdatetime,
     med_start_date, med_end_date,
     med_createddatetime, doc_createddatetime, last_dispensed_date,
     sample_expiration_date, administer_expiration_date, earliest_fill_date,
     med_code, med_name, med_coding_system,
     med_status, med_status_flag, med_indication,
     med_formulation, med_route, med_strength, med_strength_unit,
     med_frequency, med_presc_quantity, med_days_supply, med_refills,
     med_directions, med_fill_date, med_fill_type,
     discont_date, discont_reason,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type, psid, nd_extracted_date,
     udm_unq_id, enc_date_proxy)
SELECT
    'oldrxmain',
    b.oldrxid,
    e.patientid,
    DATE(e.date),
    e.encounterid,
    NULL,
    NULL,
    NULL,
    {_START},
    {_STOP},
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    b.ndc_code,
    COALESCE(c.drugname, i.itemname),
    CASE
        WHEN c.ndc     IS NOT NULL THEN 'NDC'
        WHEN i.keyname IS NOT NULL THEN i.keyname
        ELSE NULL
    END,
    CASE
        WHEN d.rxcomment IN ('Taking', 'Takes', 'Start', 'Continue')
            THEN 'Taking'
        WHEN d.rxcomment IN ('Stop', 'Not-Taking', 'Discontinued', 'Discontinue',
                             'cancel', 'Cancelled', 'cancell', 'D/C by patient',
                             'D/C by another provider')
            THEN 'Not Taking'
        WHEN d.rxcomment IN ('Refill', 'Sample/Refill')
            THEN 'Refill'
        WHEN d.rxcomment IN ('Once')
            THEN 'Stat'
        WHEN d.rxcomment IN ('never started')
            THEN 'Never Started'
        WHEN d.rxcomment IN ('Ins Not Covered, Med chg')
            THEN 'Ins Not Covered, Med chg'
        WHEN d.rxcomment IN ('Error', 'Entered in error:')
            THEN 'Errors'
        WHEN d.rxcomment IN ('Awaiting ins. approval:')
            THEN 'Yet to start'
        ELSE NULL
    END,
    '',
    '',
    ord.med_formulation,
    ord.med_route,
    ord.med_strength,
    NULL,
    ord.med_frequency,
    ord.med_pb_qty,
    ord.med_days_supply,
    ord.med_refills,
    COALESCE(NULLIF(TRIM(ora.AdditionalInstructions), ''), ora.rxnotes),
    b.FillDate,
    NULL,
    NULL,
    '',
    CURRENT_TIMESTAMP(),
    'ND',
    CURRENT_TIMESTAMP(),
    'ND',
    'eCW',
    'bronze_table',
    'Structured',
    {PSID},
    DATE(b.nd_extracted_date),
    MD5(CONCAT_WS(':',
        COALESCE({PSID},                           ''),
        COALESCE(e.patientid,                      ''),
        COALESCE(e.encounterid,                    ''),
        COALESCE(DATE(e.date),                     ''),
        COALESCE({_START},                         ''),
        COALESCE({_STOP},                          ''),
        COALESCE(b.ndc_code,                       ''),
        COALESCE(COALESCE(c.drugname, i.itemname), ''),
        COALESCE(b.oldrxid,                        '')
    )),
    COALESCE(DATE(e.date), {_START}, b.FillDate)
FROM {SOURCE_SCHEMA}.enc e
INNER JOIN {SOURCE_SCHEMA}.oldrxmain b
    ON  e.encounterid               = b.encounterid
    AND COALESCE(e.nd_ActiveFlag,  'Y') = 'Y'
    AND COALESCE(b.nd_ActiveFlag,  'Y') = 'Y'
JOIN {STAGING_PIVOT} ord
    ON  ord.oldrxid = b.oldrxid
LEFT JOIN {SOURCE_SCHEMA}.oldrxmain_addlinfo ora
    ON  ora.oldrxid       = b.oldrxid
    AND ora.nd_ActiveFlag = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.ndclookupenteries c
    ON  c.ndc                           = b.ndc_code
    AND COALESCE(c.nd_ActiveFlag,  'Y') = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.items i
    ON  i.itemid                        = b.itemid
    AND COALESCE(i.nd_ActiveFlag,  'Y') = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.rx_medication_alert d
    ON  d.encounterid                   = b.encounterid
    AND d.itemid                        = b.itemid
    AND COALESCE(d.nd_ActiveFlag,  'Y') = 'Y'
WHERE b.oldrxid >= {pk_lo}
  AND b.oldrxid <  {pk_hi}
GROUP BY
    b.oldrxid,
    e.patientid, e.encounterid, e.date,
    b.startdate, b.stopdate, b.ndc_code, b.FillDate, b.nd_extracted_date,
    c.drugname, i.itemname, c.ndc, i.keyname,
    d.rxcomment,
    ora.AdditionalInstructions, ora.rxnotes,
    ord.med_formulation, ord.med_strength, ord.med_pb_qty,
    ord.med_days_supply, ord.med_refills, ord.med_route, ord.med_frequency
"""


# ── Setup ─────────────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. Source table indexes ───────────────────────────────────────
    print("  Ensuring indexes on source tables...")
    for tbl, col in [
        ("enc",           "encounterid"),
        ("oldrxmain",     "encounterid"),
        ("oldrxmain",     "oldrxid"),
        ("oldrxdetail",   "oldrxid"),
        ("oldrxdetail",   "prop"),
        ("properties",    "propid"),
    ]:
        if not _index_exists(cur, SOURCE_SCHEMA, tbl, col):
            print(f"    Adding index on {SOURCE_SCHEMA}.{tbl}({col})...")
            cur.execute(f"CREATE INDEX idx_{col.lower()} ON {SOURCE_SCHEMA}.{tbl} ({col})")
            conn.commit()
            print(f"    done")
        else:
            print(f"    {tbl}({col}) — exists")

    # ── 2. Pivot staging: oldrxdetail properties per oldrxid ─────────
    # Grouped by oldrxid only (collapsed across dates) to prevent duplicate
    # JOIN rows in the batch INSERT.
    print("  Materializing oldrxdetail pivot (GROUP BY oldrxid)...")
    if not _table_exists(cur, STAGING_PIVOT):
        cur.execute(f"""
            CREATE TABLE {STAGING_PIVOT} AS
            SELECT
                ord.oldrxid,
                MAX(CASE WHEN prop.name = 'Formulation'        THEN ord.value END) AS med_formulation,
                MAX(CASE WHEN prop.name = 'Frequency'          THEN ord.value END) AS med_frequency,
                MAX(CASE WHEN prop.name = 'Size'               THEN ord.value END) AS med_strength,
                MAX(CASE WHEN prop.name IN ('Take', 'Amount')  THEN ord.value END) AS med_pb_qty,
                MAX(CASE WHEN prop.name = 'Duration'           THEN ord.value END) AS med_days_supply,
                SUM(CASE WHEN prop.name = 'Refills'            THEN ord.value END) AS med_refills,
                MAX(CASE WHEN prop.name = 'Route'              THEN ord.value END) AS med_route,
                MAX(DATE(ord.nd_extracted_date))                                   AS nd_extracted_date
            FROM {SOURCE_SCHEMA}.oldrxdetail ord
            JOIN {SOURCE_SCHEMA}.properties prop ON prop.propid = ord.prop
            GROUP BY ord.oldrxid
        """)
        cur.execute(f"ALTER TABLE {STAGING_PIVOT} ADD INDEX idx_oldrxid (oldrxid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PIVOT}")
    print(f"    {cur.fetchone()[0]:,} pivot rows")

    # ── 3. Destination table ──────────────────────────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            source                     VARCHAR(50)   DEFAULT NULL,
            med_id                     BIGINT        DEFAULT NULL,
            ndid                       BIGINT        DEFAULT NULL,
            enc_date                   DATE          DEFAULT NULL,
            eid                        BIGINT        DEFAULT NULL,
            written_date               DATE          DEFAULT NULL,
            med_administered_datetime  DATE          DEFAULT NULL,
            doc_orderdatetime          DATE          DEFAULT NULL,
            med_start_date             DATE          DEFAULT NULL,
            med_end_date               DATE          DEFAULT NULL,
            med_createddatetime        DATETIME      DEFAULT NULL,
            doc_createddatetime        DATETIME      DEFAULT NULL,
            last_dispensed_date        DATE          DEFAULT NULL,
            sample_expiration_date     DATE          DEFAULT NULL,
            administer_expiration_date DATE          DEFAULT NULL,
            earliest_fill_date         DATE          DEFAULT NULL,
            med_code                   VARCHAR(100)  DEFAULT NULL,
            med_name                   TEXT,
            med_coding_system          VARCHAR(20)   DEFAULT NULL,
            med_status                 VARCHAR(50)   DEFAULT NULL,
            med_status_flag            VARCHAR(50)   DEFAULT NULL,
            med_indication             VARCHAR(200)  DEFAULT NULL,
            med_formulation            VARCHAR(200)  DEFAULT NULL,
            med_route                  VARCHAR(200)  DEFAULT NULL,
            med_strength               VARCHAR(200)  DEFAULT NULL,
            med_strength_unit          VARCHAR(200)  DEFAULT NULL,
            med_frequency              VARCHAR(200)  DEFAULT NULL,
            med_presc_quantity         VARCHAR(100)  DEFAULT NULL,
            med_days_supply            VARCHAR(100)  DEFAULT NULL,
            med_refills                VARCHAR(50)   DEFAULT NULL,
            med_directions             TEXT,
            med_fill_date              DATE          DEFAULT NULL,
            med_fill_type              VARCHAR(100)  DEFAULT NULL,
            discont_date               DATE          DEFAULT NULL,
            discont_reason             VARCHAR(200)  DEFAULT NULL,
            created_datetime           DATETIME      DEFAULT NULL,
            created_by                 VARCHAR(50)   DEFAULT NULL,
            updated_datetime           DATETIME      DEFAULT NULL,
            updated_by                 VARCHAR(50)   DEFAULT NULL,
            ehr_source_name            VARCHAR(100)  DEFAULT NULL,
            source_path                VARCHAR(100)  DEFAULT NULL,
            data_type                  VARCHAR(50)   DEFAULT NULL,
            psid                       INT           DEFAULT NULL,
            nd_extracted_date          DATE          DEFAULT NULL,
            udm_unq_id                 VARCHAR(32)   DEFAULT NULL,
            enc_date_proxy             DATE          DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # ── 4. Checkpoint table ───────────────────────────────────────────
    print("  Creating checkpoint table...")
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

    # ── 5. PK staging per source ──────────────────────────────────────
    source_ranges = {}
    for src in SOURCES:
        pk      = src["pk"]
        table   = src["table"]
        staging = src["pk_staging"]
        print(f"  Building PK staging for {table}...")

        if not _table_exists(cur, staging):
            cur.execute(f"""
                CREATE TABLE {staging} AS
                SELECT {pk}
                FROM {SOURCE_SCHEMA}.{table}
                WHERE {pk} IS NOT NULL
                  AND COALESCE(nd_ActiveFlag, 'Y') = 'Y'
            """)
            cur.execute(f"ALTER TABLE {staging} ADD INDEX idx_pk ({pk})")
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")

        cur.execute(f"SELECT COUNT(*) FROM {staging}")
        count = cur.fetchone()[0]

        if count == 0:
            source_ranges[src["key"]] = []
            print(f"    0 rows — skipping")
            continue

        cur.execute(f"""
            SELECT {pk}
            FROM (
                SELECT {pk},
                       ROW_NUMBER() OVER (ORDER BY {pk}) AS rn
                FROM {staging}
            ) t
            WHERE (rn - 1) % {BATCH_SIZE} = 0
            ORDER BY {pk}
        """)
        boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]
        cur.execute(f"SELECT MAX({pk}) FROM {staging}")
        max_pk = int(cur.fetchone()[0])

        ranges = []
        for i, lo in enumerate(boundaries):
            hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
            ranges.append((lo, hi))

        source_ranges[src["key"]] = ranges
        print(f"    {count:,} rows → {len(ranges)} batches of ~{BATCH_SIZE:,}")

    cur.close()
    conn.close()
    return source_ranges


# ── Worker ────────────────────────────────────────────────────────────────────

def run_source(source, ranges, pbar):
    key  = source["key"]
    conn = get_connection()

    if is_done(conn, key):
        conn.close()
        pbar.update(len(ranges))
        return {"source": key, "status": "skipped", "rows": 0, "secs": 0}

    mark(conn, key, "running")
    t0         = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_batch_insert(pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        mark(conn, key, "done", total_rows)
        conn.close()
        return {"source": key, "status": "done",
                "rows": total_rows, "secs": round(time.time() - t0, 1)}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"source": key, "status": f"FAILED: {exc}",
                "rows": total_rows, "secs": elapsed}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  eCW Medication ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.oldrxmain  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  pivot      : {STAGING_PIVOT}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  workers    : {MAX_WORKERS}  |  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("Setup:")
    sys.stdout.flush()
    source_ranges = setup_tables()
    print()

    total_batches = sum(len(r) for r in source_ranges.values())
    if total_batches == 0:
        print("No rows to process. Exiting.")
        return

    results = []
    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
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
        print(f"  [{tag}] {r['source']:<42} {r['rows']:>10,} rows  ({r['secs']}s)")

    done    = sum(1 for r in results if r["status"] == "done")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = [r for r in results if "FAILED" in str(r["status"])]
    total   = sum(r["rows"] for r in results)

    print(f"\n{'='*70}")
    print(f"  Done: {done}  Skipped: {skipped}  Failed: {len(failed)}  |  Total rows: {total:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {STAGING_PIVOT};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    for src in SOURCES:
        print(f"    DROP TABLE IF EXISTS {src['pk_staging']};")

    if failed:
        print("\n  Failed sources:")
        for r in failed:
            print(f"    {r['source']}: {r['status']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
