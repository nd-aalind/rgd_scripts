#!/usr/bin/env python3
"""
Optimized ETL for: suven.medication
Source: Greenway — single source ClinicalPatMeds

Sources (1 INSERT job):
  1. ClinicalPatMeds — nd_activeflag='Y'
                       LEFT JOIN ClinicalDocuments + Visit + MedicationRxNormFact
                       + ERXPatMedList + Discontinued + FillStatus + Indication tables

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.med_gw_rxnorm_v1_{schema}  (MedicationRxNormFact MIN per Medid)
  - staging.med_gw_visit_v1_{schema}   (Visit active, keyed on VisitID)
  - staging.med_gw_doc_v1_{schema}     (ClinicalDocuments active, keyed on DocumentID)
  - staging.med_gw_ifdb_v1_{schema}    (MedicationIndicationFDB distinct active)
  - staging.med_gw_ia_v1_{schema}      (MedicationIndicationAssessment distinct active)
  - staging.med_gw_ipl_v1_{schema}     (MedicationIndicationProblemList distinct active)

Optimizations:
- All subquery JOINs pre-materialized once (MedicationRxNormFact GROUP BY runs once)
- Batching by actual ClinicalPatMedID PK values (sparse-ID safe)
- Checkpoint/resume per source
- InnoDB checks disabled per-session for bulk speed
- Commit after every batch
- tqdm progress bar

Usage:
    python med_new_gw.py
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
    "database":        "mind",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000
MAX_WORKERS = 1

# ── Change these two variables to run for a different schema/psid ──────────────
SOURCE_SCHEMA = "mind"
PSID          = 12

DEST_TABLE       = "udm_staging.medication_mind"
STAGING_RXNORM   = f"staging.med_gw_rxnorm_v1_{SOURCE_SCHEMA}"
STAGING_VISIT    = f"staging.med_gw_visit_v1_{SOURCE_SCHEMA}"
STAGING_DOC      = f"staging.med_gw_doc_v1_{SOURCE_SCHEMA}"
STAGING_IFDB     = f"staging.med_gw_ifdb_v1_{SOURCE_SCHEMA}"
STAGING_IA       = f"staging.med_gw_ia_v1_{SOURCE_SCHEMA}"
STAGING_IPL      = f"staging.med_gw_ipl_v1_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_med_gw_v1_{SOURCE_SCHEMA}"

SOURCES = [
    {
        "key":        "clinicalpatmed",
        "table":      "ClinicalPatMeds",
        "pk":         "ClinicalPatMedID",
        "pk_staging": f"staging.tmp_med_gw_pk_v1_{SOURCE_SCHEMA}",
    },
]


# ── Index helper ──────────────────────────────────────────────────────────────

def _ensure_index(cur, conn, full_table_name, index_name, columns, prefix_len=None):
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND index_name = %s",
        (schema, table, index_name),
    )
    if cur.fetchone()[0] > 0:
        print(f"    index {index_name} on {full_table_name} already exists — skipping")
        return
    col_list = ", ".join(f"{c}({prefix_len})" if prefix_len else c for c in columns)
    print(f"    creating index {index_name} on {full_table_name}({col_list}) ...")
    cur.execute(f"ALTER TABLE {full_table_name} ADD INDEX {index_name} ({col_list})")
    conn.commit()
    print(f"    done")


# ── Date expressions (module-level to avoid repetition in f-strings) ──────────

# med_start_date: prefer m.StartDT, fall back to erx.StartDate with safe parsing
_MED_START = (
    "CASE"
    " WHEN m.StartDT IS NOT NULL"
    "     THEN DATE(m.StartDT)"
    " WHEN erx.StartDate IS NULL"
    "     THEN NULL"
    " WHEN TRIM(erx.StartDate) IN ('', 'None', 'NONE', '0000-00-00 00:00:00')"
    "     THEN NULL"
    " WHEN STR_TO_DATE(erx.StartDate, '%Y-%m-%d %H:%i:%s') IS NOT NULL"
    "     THEN DATE(STR_TO_DATE(erx.StartDate, '%Y-%m-%d %H:%i:%s'))"
    " WHEN STR_TO_DATE(erx.StartDate, '%m-%d-%Y %H:%i:%s') IS NOT NULL"
    "     THEN DATE(STR_TO_DATE(erx.StartDate, '%m-%d-%Y %H:%i:%s'))"
    " ELSE NULL END"
)

# med_end_date: safe parsing of erx.StopDate
_MED_END = (
    "CASE"
    " WHEN erx.StopDate IS NULL"
    "     THEN NULL"
    " WHEN TRIM(erx.StopDate) IN ('', 'None', 'NONE', '0000-00-00 00:00:00')"
    "     THEN NULL"
    " WHEN STR_TO_DATE(erx.StopDate, '%Y-%m-%d %H:%i:%s') IS NOT NULL"
    "     THEN DATE(STR_TO_DATE(erx.StopDate, '%Y-%m-%d %H:%i:%s'))"
    " WHEN STR_TO_DATE(erx.StopDate, '%m-%d-%Y %H:%i:%s') IS NOT NULL"
    "     THEN DATE(STR_TO_DATE(erx.StopDate, '%m-%d-%Y %H:%i:%s'))"
    " ELSE NULL END"
)


# ── Batch INSERT builder ──────────────────────────────────────────────────────

def _build_gw_insert(pk_lo, pk_hi):
    return f"""
INSERT INTO {DEST_TABLE}
    (source, med_id, ndid, enc_date, eid,
     written_date, med_administered_datetime, doc_orderdatetime,
     med_start_date, med_end_date, med_createddatetime, doc_createddatetime,
     last_dispensed_date, sample_expiration_date, administer_expiration_date,
     earliest_fill_date, med_code, med_name, med_coding_system,
     med_status, med_status_flag, med_indication,
     med_formulation, med_route, med_strength, med_strength_unit,
     med_frequency, med_presc_quantity, med_days_supply, med_refills,
     med_directions, med_fill_date, med_fill_type,
     discont_date, discont_reason,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type, psid, nd_extracted_date,
     udm_unq_id, enc_date_proxy)
SELECT
    'clinicalpatmed',
    m.ClinicalPatMedID,
    m.PatientID,
    DATE(v.FromDateTime),
    COALESCE(c.VisitID, m.VisitID),
    NULL,
    NULL,
    NULL,
    {_MED_START},
    {_MED_END},
    m.CreateDate,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    b.RXNORM,
    COALESCE(m.MedicationName, b.RXNORMDISPLAY),
    NULL,
    NULL,
    NULL,
    COALESCE(ifdb.FDBMedicalConditionDesc, ia.ICDDesc, ipl.PatHistItemICDDesc),
    NULL,
    COALESCE(m.Route, erx.SigRoute),
    COALESCE(
        NULLIF(CASE WHEN TRIM(LOWER(m.MedicationStrength))  IN ('none', '') THEN '' ELSE TRIM(m.MedicationStrength)  END, ''),
        NULLIF(CASE WHEN TRIM(LOWER(erx.DrugStrength))      IN ('none', '') THEN '' ELSE TRIM(erx.DrugStrength)      END, '')
    ),
    CASE
        WHEN TRIM(LOWER(m.MedicationStrengthUnit)) = 'none'
             OR TRIM(m.MedicationStrengthUnit) = ''
        THEN NULL
        ELSE TRIM(m.MedicationStrengthUnit)
    END,
    m.FrequencyCode,
    COALESCE(
        CONCAT(m.DispenseAmount, ' ', m.DispenseUnit),
        CONCAT(erx.SigQuantity,  ' ', erx.SigQuantityUnit)
    ),
    m.Duration,
    COALESCE(m.NumRefills, erx.Refills),
    m.SIG COLLATE utf8mb4_general_ci,
    f.StatusDate,
    fs.StatusDescription,
    md.DiscontinuedDate,
    dcr.Description,
    CURRENT_TIMESTAMP(),
    'ND',
    CURRENT_TIMESTAMP(),
    'ND',
    'Greenway',
    'bronze_table',
    'Structured',
    {PSID},
    m.nd_extracted_date,
    MD5(CONCAT_WS(':',
        COALESCE({PSID},                                          ''),
        COALESCE(m.PatientID,                                     ''),
        COALESCE(COALESCE(c.VisitID, m.VisitID),                  ''),
        COALESCE(DATE(v.FromDateTime),                            ''),
        COALESCE({_MED_START},                                    ''),
        COALESCE({_MED_END},                                      ''),
        COALESCE(b.RXNORM,                                        ''),
        COALESCE(COALESCE(m.MedicationName, b.RXNORMDISPLAY),     '')
    )),
    COALESCE(DATE(v.FromDateTime), {_MED_START}, f.StatusDate, m.CreateDate)
FROM {SOURCE_SCHEMA}.ClinicalPatMeds m
LEFT JOIN {STAGING_DOC} c
    ON  c.DocumentID = m.OrderingDocumentID
    AND c.PatientID  = m.PatientID
LEFT JOIN {STAGING_VISIT} v
    ON  v.VisitID = COALESCE(c.VisitID, m.VisitID)
LEFT JOIN {STAGING_RXNORM} b
    ON  b.Medid = m.MedicationID
LEFT JOIN {SOURCE_SCHEMA}.ERXPatMedList erx
    ON  erx.ClinicalPatMedID = m.ClinicalPatMedID
    AND erx.nd_activeflag    = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.ClinicalPatMedsDiscontinued md
    ON  md.ClinicalPatMedID = m.ClinicalPatMedID
    AND md.nd_activeflag    = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.ClinicalDiscontinuedReason dcr
    ON  dcr.DiscontinuedReasonid = md.DiscontinuedReasonID
    AND dcr.nd_activeflag        = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.ClinicalPatMedsFillStatus f
    ON  f.ClinicalPatMedID = m.ClinicalPatMedID
    AND f.nd_activeflag    = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.RXFillStatus fs
    ON  fs.RXFillStatusID = f.RxFillStatusID
    AND fs.nd_activeflag  = 'Y'
LEFT JOIN {STAGING_IFDB} ifdb
    ON  ifdb.ClinicalPatMedID = m.ClinicalPatMedID
LEFT JOIN {STAGING_IA} ia
    ON  ia.ClinicalPatMedID = m.ClinicalPatMedID
LEFT JOIN {STAGING_IPL} ipl
    ON  ipl.ClinicalPatMedID = m.ClinicalPatMedID
WHERE m.nd_activeflag = 'Y'
  AND m.ClinicalPatMedID >= {pk_lo}
  AND m.ClinicalPatMedID <  {pk_hi}
"""


def build_batch_insert(source, pk_lo, pk_hi):
    return _build_gw_insert(pk_lo, pk_hi)


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


# ── Setup ─────────────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. Indexes on source join/filter columns ──────────────────────
    print("  Ensuring indexes on source tables...")
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.ClinicalPatMeds",
                  "idx_clinicalpatmedid",  ["ClinicalPatMedID"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.ClinicalPatMeds",
                  "idx_activeflag",        ["nd_activeflag"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.ClinicalPatMeds",
                  "idx_orderingdocid",     ["OrderingDocumentID"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.ClinicalPatMeds",
                  "idx_medicationid",      ["MedicationID"])

    # ── 2. STAGING_RXNORM: MedicationRxNormFact MIN per Medid ─────────
    print("  Materializing MedicationRxNormFact lookup (MIN RxNorm per Medid)...")
    if not _table_exists(cur, STAGING_RXNORM):
        cur.execute(f"""
            CREATE TABLE {STAGING_RXNORM} AS
            SELECT Medid,
                   MIN(RxNorm)        AS RXNORM,
                   MIN(RxNormDisplay) AS RXNORMDISPLAY
            FROM {SOURCE_SCHEMA}.MedicationRxNormFact
            WHERE nd_activeflag = 'Y'
            GROUP BY Medid
        """)
        cur.execute(f"ALTER TABLE {STAGING_RXNORM} ADD INDEX idx_medid (Medid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_RXNORM}")
    print(f"    {cur.fetchone()[0]:,} RxNorm rows")

    # ── 3. STAGING_VISIT: Visit filtered active ───────────────────────
    print("  Materializing Visit lookup (nd_activeflag='Y')...")
    if not _table_exists(cur, STAGING_VISIT):
        cur.execute(f"""
            CREATE TABLE {STAGING_VISIT} AS
            SELECT VisitID, FromDateTime
            FROM {SOURCE_SCHEMA}.Visit
            WHERE nd_activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_VISIT} ADD INDEX idx_visitid (VisitID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_VISIT}")
    print(f"    {cur.fetchone()[0]:,} Visit rows")

    # ── 4. STAGING_DOC: ClinicalDocuments filtered active ────────────
    print("  Materializing ClinicalDocuments lookup (nd_activeflag='Y')...")
    if not _table_exists(cur, STAGING_DOC):
        cur.execute(f"""
            CREATE TABLE {STAGING_DOC} AS
            SELECT DocumentID, VisitID, PatientID
            FROM {SOURCE_SCHEMA}.ClinicalDocuments
            WHERE nd_activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_DOC} ADD INDEX idx_docid (DocumentID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_DOC}")
    print(f"    {cur.fetchone()[0]:,} ClinicalDocuments rows")

    # ── 5. STAGING_IFDB: MedicationIndicationFDB distinct active ─────
    print("  Materializing MedicationIndicationFDB lookup...")
    if not _table_exists(cur, STAGING_IFDB):
        cur.execute(f"""
            CREATE TABLE {STAGING_IFDB} AS
            SELECT DISTINCT ClinicalPatMedID, FDBMedicalConditionDesc
            FROM {SOURCE_SCHEMA}.MedicationIndicationFDB
            WHERE FDBMedicalConditionDesc IS NOT NULL
              AND nd_activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_IFDB} ADD INDEX idx_cpmedid (ClinicalPatMedID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_IFDB}")
    print(f"    {cur.fetchone()[0]:,} IFDB rows")

    # ── 6. STAGING_IA: MedicationIndicationAssessment distinct active ─
    print("  Materializing MedicationIndicationAssessment lookup...")
    if not _table_exists(cur, STAGING_IA):
        cur.execute(f"""
            CREATE TABLE {STAGING_IA} AS
            SELECT DISTINCT ClinicalPatMedID, ICDDesc
            FROM {SOURCE_SCHEMA}.MedicationIndicationAssessment
            WHERE ICDDesc IS NOT NULL
              AND nd_activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_IA} ADD INDEX idx_cpmedid (ClinicalPatMedID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_IA}")
    print(f"    {cur.fetchone()[0]:,} IA rows")

    # ── 7. STAGING_IPL: MedicationIndicationProblemList distinct active
    print("  Materializing MedicationIndicationProblemList lookup...")
    if not _table_exists(cur, STAGING_IPL):
        cur.execute(f"""
            CREATE TABLE {STAGING_IPL} AS
            SELECT DISTINCT ClinicalPatMedID, PatHistItemICDDesc
            FROM {SOURCE_SCHEMA}.MedicationIndicationProblemList
            WHERE PatHistItemICDDesc IS NOT NULL
              AND nd_activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_IPL} ADD INDEX idx_cpmedid (ClinicalPatMedID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_IPL}")
    print(f"    {cur.fetchone()[0]:,} IPL rows")

    # ── 8. Destination table ──────────────────────────────────────────
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

    # ── 9. Checkpoint table ───────────────────────────────────────────
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

    # ── 10. PK staging per source ─────────────────────────────────────
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
                  AND nd_activeflag = 'Y'
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
            sql = build_batch_insert(source, pk_lo, pk_hi)
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
    print(f"  Greenway Medication ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.ClinicalPatMeds  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
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
    print(f"    DROP TABLE IF EXISTS {STAGING_RXNORM};")
    print(f"    DROP TABLE IF EXISTS {STAGING_VISIT};")
    print(f"    DROP TABLE IF EXISTS {STAGING_DOC};")
    print(f"    DROP TABLE IF EXISTS {STAGING_IFDB};")
    print(f"    DROP TABLE IF EXISTS {STAGING_IA};")
    print(f"    DROP TABLE IF EXISTS {STAGING_IPL};")
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
