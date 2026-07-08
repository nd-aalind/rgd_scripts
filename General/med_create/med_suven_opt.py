#!/usr/bin/env python3
"""
Optimized ETL: Build FCN medications from an eCW source schema.

Source tables (all under SOURCE_SCHEMA):
  enc, oldrxmain, oldrxdetail_pivot_new, oldrxmain_addlinfo,
  ndclookupenteries, items, rx_medication_alert

Patient filter: PATIENT_LIST_TABLE (explicit schema + dbname + ActiveFlag)

Target table: DEST_TABLE (CREATE + INSERT, append-safe)

Configure at the top before running:
  SOURCE_SCHEMA        — e.g. "suven", "kinsula_leq"
  PATIENT_LIST_TABLE   — full schema.table of the patient list
  PATIENT_LIST_DBNAME  — dbname value to filter the patient list
  DEST_TABLE           — target table to INSERT into
  PSID                 — psid value to insert

Strategy:
  1. Ensure indexes on all join/filter columns before batching.
  2. Pre-materialize eligible encounters (enc JOIN patient_list, enc_date parsed)
     into STAGING_ENC — eliminates the enc + patient_list JOIN at batch time.
  3. Pre-materialize eligible oldrxids (oldrxmain JOIN STAGING_ENC)
     into STAGING_PK — batch boundaries sampled from this small table.
  4. Batch INSERT by oldrxid ranges joining all source tables.
  5. Checkpoint/resume — re-run skips if already completed.

Usage:
    python med_suven_opt.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_INTERNAL_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_INTERNAL_USER"),
    "password":        os.environ.get("DB_INTERNAL_PASSWORD"),
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

SOURCE_SCHEMA        = "fcn_latest"                        # ← change per run
PATIENT_LIST_TABLE   = "suven.pateint_list_21thapri"  # ← explicit schema.table
PATIENT_LIST_DBNAME  = "fcn"                            # ← dbname filter on patient list
DEST_TABLE           = "suven.fcn_medications_22new"  # ← target table (append mode)
PSID                 = 8

BATCH_SIZE = 50_000
BATCH_KEY  = "oldrxid"

# ── Derived staging / checkpoint names (namespaced by source schema) ──
_sfx = SOURCE_SCHEMA

STAGING_ENC      = f"staging.med_suven_enc_{_sfx}"
STAGING_PK       = f"staging.med_suven_pk_{_sfx}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_med_suven_{_sfx}"
CHECKPOINT_KEY   = f"med_suven.{_sfx}"


# ── Helpers ───────────────────────────────────────────────────────────

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
        "WHERE table_schema = %s AND table_name = %s "
        "  AND column_name = %s AND seq_in_index = 1",
        (schema, table, column),
    )
    return cur.fetchone()[0] > 0


def _build_ranges(cur, staging_pk):
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
            (source_key, status, rows_inserted, started_at, completed_at, error_msg)
        VALUES (%s, %s, %s, NOW(), IF(%s = 'done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status        = VALUES(status),
            rows_inserted = VALUES(rows_inserted),
            completed_at  = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg     = VALUES(error_msg)
    """, (CHECKPOINT_KEY, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Index setup ────────────────────────────────────────────────────────

def ensure_indexes(cur, conn):
    """Ensure all join/filter columns are indexed. Skips silently if already present."""
    _pl_schema, _pl_table = PATIENT_LIST_TABLE.split(".", 1)

    needed = [
        # patient list
        (_pl_schema,   _pl_table,                  "patientid"),
        # enc
        (SOURCE_SCHEMA, "enc",                     "encounterid"),
        (SOURCE_SCHEMA, "enc",                     "nd_ActiveFlag"),
        # oldrxmain
        (SOURCE_SCHEMA, "oldrxmain",               "oldrxid"),
        (SOURCE_SCHEMA, "oldrxmain",               "encounterid"),
        (SOURCE_SCHEMA, "oldrxmain",               "nd_ActiveFlag"),
        (SOURCE_SCHEMA, "oldrxmain",               "ndc_code"),
        (SOURCE_SCHEMA, "oldrxmain",               "itemid"),
        # oldrxdetail_pivot_new
        (SOURCE_SCHEMA, "oldrxdetail_pivot_new",   "oldrxid"),
        # oldrxmain_addlinfo
        (SOURCE_SCHEMA, "oldrxmain_addlinfo",      "oldrxid"),
        # ndclookupenteries
        (SOURCE_SCHEMA, "ndclookupenteries",       "ndc"),
        # items
        (SOURCE_SCHEMA, "items",                   "itemid"),
        # rx_medication_alert
        (SOURCE_SCHEMA, "rx_medication_alert",     "encounterid"),
        (SOURCE_SCHEMA, "rx_medication_alert",     "itemid"),
    ]

    print("  Checking source table indexes...")
    created = []
    for schema, table, col in needed:
        if not _index_exists(cur, schema, table, col):
            idx_name = f"idx_medsuven_{col.lower()}"
            print(f"    creating index on {schema}.{table}({col})...")
            try:
                cur.execute(
                    f"ALTER TABLE `{schema}`.`{table}` ADD INDEX `{idx_name}` (`{col}`)"
                )
                conn.commit()
                created.append(f"{table}.{col}")
            except Exception as e:
                print(f"    WARNING: could not create index on {schema}.{table}({col}): {e}")

    if created:
        print(f"    created {len(created)} index(es): {', '.join(created)}")
    else:
        print("    all indexes already present")


# ── Batch INSERT builder ───────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    """
    Batch INSERT for oldrxid range [pk_lo, pk_hi).
    enc + patient_list are pre-joined in STAGING_ENC (enc_date already parsed).
    oldrxid range comes from STAGING_PK (pre-filtered eligible rows).
    """
    return f"""
INSERT INTO {DEST_TABLE}
SELECT
    'oldrxmain'                                                               AS source,
    b.oldrxid                                                                 AS med_id,
    ep.patientID                                                              AS ndid,
    ep.encounterid                                                            AS eid,
    ep.enc_date,
    NULL                                                                      AS written_date,
    NULL                                                                      AS med_administered_datetime,
    NULL                                                                      AS doc_orderdatetime,
    CASE
        WHEN CAST(b.startdate AS CHAR) = 'None' THEN NULL
        WHEN YEAR(b.startdate) < 1991           THEN NULL
        ELSE DATE(LEFT(b.startdate, 10))
    END                                                                       AS med_start_date,
    CASE
        WHEN CAST(b.stopdate AS CHAR) = 'None'  THEN NULL
        WHEN YEAR(b.stopdate) < 1991            THEN NULL
        ELSE DATE(LEFT(b.stopdate, 10))
    END                                                                       AS med_end_date,
    NULL                                                                      AS med_createddatetime,
    NULL                                                                      AS doc_createddatetime,
    NULL                                                                      AS last_dispensed_date,
    NULL                                                                      AS sample_expiration_date,
    NULL                                                                      AS administer_expiration_date,
    NULL                                                                      AS earliest_fill_date,
    b.ndc_code                                                                AS med_code,
    COALESCE(c.drugname, i.itemname)                                          AS med_name,
    CASE
        WHEN c.ndc IS NOT NULL     THEN 'NDC'
        WHEN i.keyname IS NOT NULL THEN i.keyname
        ELSE NULL
    END                                                                       AS med_coding_system,
    CASE
        WHEN d.rxcomment IN ('Taking','Takes','Start','Continue')
            THEN 'Taking'
        WHEN d.rxcomment IN ('Stop','Not-Taking','Discontinued','Discontinue','cancel',
                             'Cancelled','cancell','D/C by patient','D/C by another provider')
            THEN 'Not Taking'
        WHEN d.rxcomment IN ('Refill','Sample/Refill')
            THEN 'Refill'
        WHEN d.rxcomment IN ('Once')
            THEN 'Stat'
        WHEN d.rxcomment IN ('never started')
            THEN 'Never Started'
        WHEN d.rxcomment IN ('Ins Not Covered, Med chg')
            THEN 'Ins Not Covered, Med chg'
        WHEN d.rxcomment IN ('Error','Entered in error:')
            THEN 'Errors'
        WHEN d.rxcomment IN ('Awaiting ins. approval:')
            THEN 'Yet to start'
        ELSE NULL
    END                                                                       AS med_status,
    ''                                                                        AS med_status_flag,
    ''                                                                        AS med_indication,
    ord.med_formulation,
    ord.med_route,
    ord.med_strength,
    NULL                                                                      AS med_strength_unit,
    ord.med_frequency,
    ord.med_pb_qty,
    ord.med_days_supply,
    ord.med_refills,
    COALESCE(NULLIF(TRIM(ora.additionalinstructions), ''), ora.rxnotes)       AS med_directions,
    b.FillDate                                                                AS fill_date,
    NULL                                                                      AS med_fill_type,
    NULL                                                                      AS discont_date,
    ''                                                                        AS discont_reason,
    CURRENT_TIMESTAMP()                                                       AS created_datetime,
    'ND'                                                                      AS created_by,
    CURRENT_TIMESTAMP()                                                       AS updated_datetime,
    'ND'                                                                      AS updated_by,
    'eCW'                                                                     AS ehr_source_name,
    'bronze_table'                                                            AS source_path,
    'Structured'                                                              AS data_type,
    {PSID}                                                                    AS psid,
    b.nd_extracted_date
FROM {STAGING_PK} pk
JOIN {SOURCE_SCHEMA}.oldrxmain b
    ON pk.{BATCH_KEY} = b.{BATCH_KEY}
    AND b.nd_ActiveFlag = 'Y'
JOIN {STAGING_ENC} ep
    ON b.encounterid = ep.encounterid
LEFT JOIN {SOURCE_SCHEMA}.oldrxdetail_pivot_new ord
    ON ord.oldrxid = b.oldrxid
LEFT JOIN {SOURCE_SCHEMA}.oldrxmain_addlinfo ora
    ON b.oldrxid = ora.oldrxid
LEFT JOIN {SOURCE_SCHEMA}.ndclookupenteries c
    ON b.ndc_code = c.ndc AND c.nd_ActiveFlag = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.items i
    ON b.itemid = i.itemid AND i.nd_ActiveFlag = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.rx_medication_alert d
    ON b.encounterid = d.encounterid AND b.itemid = d.itemid AND d.nd_ActiveFlag = 'Y'
WHERE pk.{BATCH_KEY} >= {pk_lo}
  AND pk.{BATCH_KEY} <  {pk_hi}
GROUP BY
    b.oldrxid, ep.patientID, ep.encounterid, ep.enc_date,
    b.startdate, b.stopdate, b.ndc_code, c.drugname, i.itemname, c.ndc, i.keyname,
    d.rxcomment, ora.additionalinstructions, ora.rxnotes, b.FillDate,
    ord.med_formulation, ord.med_strength, ord.med_pb_qty, ord.med_days_supply,
    ord.med_refills, ord.med_route, ord.med_frequency
"""


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    _pl_schema, _pl_table = PATIENT_LIST_TABLE.split(".", 1)

    # ── 0. Ensure indexes ─────────────────────────────────────────────
    ensure_indexes(cur, conn)

    # ── 1. Destination table (create if not exists — append mode) ─────
    print(f"  Checking destination table {DEST_TABLE}...")
    if not _table_exists(cur, DEST_TABLE):
        cur.execute(f"""
            CREATE TABLE {DEST_TABLE} (
                source                     VARCHAR(50)   DEFAULT NULL,
                med_id                     BIGINT        DEFAULT NULL,
                ndid                       BIGINT        DEFAULT NULL,
                eid                        BIGINT        DEFAULT NULL,
                enc_date                   DATE          DEFAULT NULL,
                written_date               DATE          DEFAULT NULL,
                med_administered_datetime  DATETIME      DEFAULT NULL,
                doc_orderdatetime          DATETIME      DEFAULT NULL,
                med_start_date             DATE          DEFAULT NULL,
                med_end_date               DATE          DEFAULT NULL,
                med_createddatetime        DATETIME      DEFAULT NULL,
                doc_createddatetime        DATETIME      DEFAULT NULL,
                last_dispensed_date        DATE          DEFAULT NULL,
                sample_expiration_date     DATE          DEFAULT NULL,
                administer_expiration_date DATE          DEFAULT NULL,
                earliest_fill_date         DATE          DEFAULT NULL,
                med_code                   VARCHAR(255)  DEFAULT NULL,
                med_name                   VARCHAR(500)  DEFAULT NULL,
                med_coding_system          VARCHAR(100)  DEFAULT NULL,
                med_status                 VARCHAR(100)  DEFAULT NULL,
                med_status_flag            VARCHAR(100)  DEFAULT NULL,
                med_indication             VARCHAR(500)  DEFAULT NULL,
                med_formulation            VARCHAR(500)  DEFAULT NULL,
                med_route                  VARCHAR(500)  DEFAULT NULL,
                med_strength               VARCHAR(500)  DEFAULT NULL,
                med_strength_unit          VARCHAR(500)  DEFAULT NULL,
                med_frequency              VARCHAR(500)  DEFAULT NULL,
                med_pb_qty                 VARCHAR(500)  DEFAULT NULL,
                med_days_supply            VARCHAR(500)  DEFAULT NULL,
                med_refills                VARCHAR(500)  DEFAULT NULL,
                med_directions             LONGTEXT,
                fill_date                  DATE          DEFAULT NULL,
                med_fill_type              VARCHAR(100)  DEFAULT NULL,
                discont_date               DATE          DEFAULT NULL,
                discont_reason             VARCHAR(500)  DEFAULT NULL,
                created_datetime           DATETIME      DEFAULT NULL,
                created_by                 VARCHAR(50)   DEFAULT NULL,
                updated_datetime           DATETIME      DEFAULT NULL,
                updated_by                 VARCHAR(50)   DEFAULT NULL,
                ehr_source_name            VARCHAR(100)  DEFAULT NULL,
                source_path                VARCHAR(100)  DEFAULT NULL,
                data_type                  VARCHAR(50)   DEFAULT NULL,
                psid                       INT           DEFAULT NULL,
                nd_extracted_date          DATE          DEFAULT NULL
            ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC
        """)
        conn.commit()
        print("    created (empty)")
    else:
        print("    already exists — will append")
        # Widen any columns that may have been created with narrow definitions
        widen = [
            ("med_formulation",  "VARCHAR(500)"),
            ("med_route",        "VARCHAR(500)"),
            ("med_strength",     "VARCHAR(500)"),
            ("med_strength_unit","VARCHAR(500)"),
            ("med_frequency",    "VARCHAR(500)"),
            ("med_pb_qty",       "VARCHAR(500)"),
            ("med_days_supply",  "VARCHAR(500)"),
            ("med_refills",      "VARCHAR(500)"),
            ("med_directions",   "LONGTEXT"),
        ]
        for col, col_type in widen:
            try:
                cur.execute(
                    f"ALTER TABLE {DEST_TABLE} MODIFY COLUMN `{col}` {col_type} DEFAULT NULL"
                )
                conn.commit()
            except Exception:
                pass  # column may not exist yet or already wide enough

    # ── 2. Encounter staging — enc JOIN patient_list (enc_date pre-parsed) ──
    print(f"  Creating encounter staging {STAGING_ENC}...")
    if not _table_exists(cur, STAGING_ENC):
        cur.execute(f"""
            CREATE TABLE {STAGING_ENC} AS
            SELECT
                e.encounterid,
                e.patientID,
                CASE
                    WHEN CAST(e.date AS CHAR) IN ('None', '', '0000-00-00') THEN NULL
                    ELSE DATE(e.date)
                END AS enc_date
            FROM {SOURCE_SCHEMA}.enc e
            INNER JOIN {PATIENT_LIST_TABLE} pl
                ON pl.patientid = e.patientID
                AND pl.dbname   = '{PATIENT_LIST_DBNAME}'
                AND pl.ActiveFlag = 'Y'
            WHERE e.nd_ActiveFlag = 'Y'
              AND e.encounterid IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_ENC} ADD INDEX idx_enc_eid (encounterid)")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_ENC}")
        print(f"    created ({cur.fetchone()[0]:,} eligible encounters)")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_ENC}")
        print(f"    already exists, reusing ({cur.fetchone()[0]:,} rows)")

    # ── 3. PK staging — eligible oldrxids ────────────────────────────
    print(f"  Creating PK staging {STAGING_PK}...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT b.{BATCH_KEY}
            FROM {SOURCE_SCHEMA}.oldrxmain b
            INNER JOIN {STAGING_ENC} ep ON b.encounterid = ep.encounterid
            WHERE b.nd_ActiveFlag = 'Y'
              AND b.{BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
        print(f"    created ({cur.fetchone()[0]:,} eligible oldrxids)")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
        print(f"    already exists, reusing ({cur.fetchone()[0]:,} rows)")

    # ── 4. Checkpoint table ──────────────────────────────────────────
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

    # ── 5. Batch ranges ──────────────────────────────────────────────
    ranges, total = _build_ranges(cur, STAGING_PK)
    print(f"    {total:,} rows → {len(ranges)} batches of ~{BATCH_SIZE:,}")

    cur.close()
    conn.close()
    return ranges


# ── Runner ─────────────────────────────────────────────────────────────

def run_insert(ranges, pbar):
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


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  FCN Medications ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source_schema    : {SOURCE_SCHEMA}")
    print(f"  patient_list     : {PATIENT_LIST_TABLE}  (dbname='{PATIENT_LIST_DBNAME}')")
    print(f"  dest             : {DEST_TABLE}")
    print(f"  psid             : {PSID}")
    print(f"  batch_size       : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Setting up tables...", flush=True)
    ranges = setup_tables()

    if not ranges:
        print("\n  No eligible oldrxids found. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc=SOURCE_SCHEMA, unit="batch") as pbar:
        result = run_insert(ranges, pbar)

    print()
    if result["status"] == "done":
        tag = " DONE"
    elif result["status"] == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"

    print(f"\n{'='*70}")
    print(f"  [{tag}] {SOURCE_SCHEMA:<15}  {result['rows']:>10,} rows inserted  ({result['secs']}s)")
    if result["status"].startswith("FAILED"):
        print(f"  ERROR: {result['status']}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    for t in [STAGING_ENC, STAGING_PK, CHECKPOINT_TABLE]:
        print(f"    DROP TABLE IF EXISTS {t};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
