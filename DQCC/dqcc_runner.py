#!/usr/bin/env python3
"""
DQCC Runner — Data Quality Control Check Execution Engine

Reads active rules from dqqc_rgd.dq_rule, executes completeness checks
against rgd_udm_silver tables in parallel, and stores results in dqqc_rgd.dq_run_result.

Usage:
    python dqcc_runner.py
"""

import pymysql
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

DB_CONFIG = {
    'host':            '172.16.2.42',
    'port':            3306,
    'user':            'nd-root-mysql',
    'password':        'kmsamd89undsd4',
    'charset':         'utf8mb4',
    'autocommit':      False,
    'connect_timeout': 30,
}

DQ_SCHEMA     = 'dqqc_rgd'
SILVER_SCHEMA = 'incremental_test'
RESULT_TABLE  = f'{DQ_SCHEMA}.dq_run_result_inc_run_new_2'
RULE_TABLE    = f'{DQ_SCHEMA}.dq_rule_test'
TRIGGERED_BY  = 'DQ_JOB'
MAX_WORKERS   = 4

# ─────────────────────────────────────────────────────────────────────────────
# Table Registry
# Single place to rename / remap any silver-layer table.
# Keys are logical names used throughout RULE_CONFIG below.
# ─────────────────────────────────────────────────────────────────────────────

TABLES = {
    'patient_demographics': 'patients',
    'encounters':           'encounters',
    'diagnosis':            'diagnosis',
    'procedures':           'procedures',
    'medication':           ['medications_part1'],  # list = split table
    'labs':                 'labs',
    'radiology':            'radiology',
    'vitals':               'vitals',
    'allergies':            'allergies',
    'examination':          'examination',
    'ros':                  'ros',
}


def _expand(key):
    """Return a flat list of physical table names for a logical TABLES key.
    Works whether the value is a str (single table) or list (sharded table).
    """
    val = TABLES[key]
    return val if isinstance(val, list) else [val]


def _expand_with_col(key, col):
    """Return [(physical_table, col), ...] for a logical key, expanding shards."""
    return [(t, col) for t in _expand(key)]


def _as_list(val):
    """Normalise a str or list[str] table value to always be a list."""
    return val if isinstance(val, list) else [val]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Rule Static Metadata
# Thresholds are loaded from dq_rule at runtime; 'default_threshold' is fallback.
#
# check_type values:
#   'completeness'              — VARCHAR: col IS NULL OR col = ''
#   'date_completeness'         — DATE/DATETIME: col IS NULL OR CAST(col AS CHAR) = ''
#   'multi_completeness'        — same column across multiple tables
#   'multi_completeness_custom' — different column name per table (e.g. eid vs enc_id)
#   'multi_column_completeness' — all N columns non-null in a single table
#   'multi_date_per_table'      — date completeness, one result row per (table, column)
# ─────────────────────────────────────────────────────────────────────────────

RULE_CONFIG = {
    'COMP-001': {
        'check_type': 'multi_completeness',
        'column': 'ndid',
        # All 11 clinical tables — add/remove logical keys from TABLES as needed
        'tables': [
            TABLES['patient_demographics'], TABLES['encounters'],    TABLES['diagnosis'],
            TABLES['procedures'],           *_expand('medication'),  TABLES['labs'],
            TABLES['radiology'],            TABLES['vitals'],        TABLES['allergies'],
            TABLES['examination'],          TABLES['ros'],
        ],
        'default_threshold': 100.0,
    },
    'COMP-002': {
        'check_type': 'multi_completeness_custom',
        'column': 'eid',   # logical label stored in column_name field
        # (physical_table, column_in_that_table)
        'tables_columns': [
            (TABLES['encounters'],   'eid'),
            (TABLES['diagnosis'],    'eid'),
            (TABLES['procedures'],   'eid'),
            *_expand_with_col('medication', 'eid'),   # expands to one entry per shard
            (TABLES['labs'],         'eid'),
            (TABLES['radiology'],    'eid'),
            (TABLES['vitals'],       'eid'),
            (TABLES['allergies'],    'eid'),
            (TABLES['examination'],  'eid'),
            (TABLES['ros'],          'eid'),
        ],
        'default_threshold': 100.0,
    },
    'COMP-003': {
        'check_type': 'multi_date_per_table',
        'column': 'date_fields',
        'tables_columns': [
            (TABLES['encounters'],  'enc_date'),
            (TABLES['diagnosis'],   'diag_date'),
            *_expand_with_col('medication', 'med_start_date'),  # one entry per shard
            (TABLES['labs'],        'result_date'),
            (TABLES['procedures'],  'proc_start_date'),
            (TABLES['radiology'],   'img_date'),
            (TABLES['vitals'],      'vital_date'),
        ],
        'default_threshold': 98.0,
    },
    'COMP-004': {
        'check_type': 'completeness',
        'table':  TABLES['diagnosis'],
        'column': 'diag_code',
        'default_threshold': 95.0,
    },
    'COMP-005': {
        'check_type': 'completeness',
        'table':  TABLES['medication'],
        'column': 'med_code',
        'default_threshold': 95.0,
    },
    'COMP-008': {
        'check_type': 'multi_column_completeness',
        'table':        TABLES['labs'],
        'columns':      ['result_name', 'result_value', 'result_unit'],
        'date_columns': ['result_date'],                         # DATE column — IS NULL only
        'column':       'lab_core_fields',
        'default_threshold': 95.0,
    },
    'COMP-009': {
        'check_type': 'multi_column_completeness',
        'table':        TABLES['medication'],
        'columns':      ['med_name', 'med_strength'],  # string columns
        'date_columns': ['med_start_date'],             # DATE columns — IS NULL only
        'column':       'med_details',
        'default_threshold': 90.0,
    },
    'COMP-010': {
        'check_type': 'completeness',
        'table':  TABLES['patient_demographics'],
        'column': 'pat_deceased_status',
        'default_threshold': 100.0,
    },
    'COMP-011': {
        'check_type': 'completeness',
        'table':  TABLES['patient_demographics'],
        'column': 'gender',
        'default_threshold': 100.0,
    },
    'COMP-012': {
        'check_type': 'date_completeness',
        'table':  TABLES['patient_demographics'],
        'column': 'dob',
        'default_threshold': 100.0,
    },
    'COMP-013': {
        'check_type': 'date_completeness',
        'table':  TABLES['encounters'],
        'column': 'enc_date',
        'default_threshold': 100.0,
    },
    'COMP-014': {
        'check_type': 'date_completeness',
        'table':  TABLES['diagnosis'],
        'column': 'diag_date',
        'default_threshold': 95.0,
    },
    'COMP-015': {
        'check_type': 'date_completeness',
        'table':  TABLES['medication'],
        'column': 'med_start_date',
        'default_threshold': 90.0,
    },
    'COMP-016': {
        'check_type': 'date_completeness',
        'table':  TABLES['labs'],
        'column': 'result_date',
        'default_threshold': 95.0,
    },
    'COMP-017': {
        'check_type': 'date_completeness',
        'table':  TABLES['procedures'],
        'column': 'proc_start_date',
        'default_threshold': 95.0,
    },
    'COMP-020': {
        'check_type': 'multi_completeness',
        'column': 'incremental_id',
        'tables': [
            TABLES['patient_demographics'], TABLES['encounters'],    TABLES['diagnosis'],
            TABLES['procedures'],           *_expand('medication'),  TABLES['labs'],
            TABLES['radiology'],            TABLES['vitals'],        TABLES['allergies'],
            TABLES['examination'],          TABLES['ros'],
        ],
        'default_threshold': 100.0,
    },
}

# Rules skipped in automated run:
#   COMP-006/007/018/019 — require NLP / derived cohort logic
#   COMP-020             — incremental_id column not yet present in silver tables
SKIPPED_RULES = {'COMP-006', 'COMP-007', 'COMP-018', 'COMP-019', 'COMP-020'}

# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


def load_rule_thresholds():
    """
    Load threshold values from dq_rule for active rules.
    Returns dict: rule_id -> threshold (float).
    Falls back to empty dict if table doesn't exist yet.
    """
    conn = get_connection()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(f"""
                SELECT rule_id, threshold
                FROM   {RULE_TABLE}
                WHERE  is_active = 'Y'
            """)
            return {row['rule_id']: float(row['threshold']) for row in cur.fetchall()}
    except Exception as exc:
        log.warning(f"Could not read {RULE_TABLE} ({exc}). Using built-in defaults.")
        return {}
    finally:
        conn.close()

# ─────────────────────────────────────────────────────────────────────────────
# SQL execution helpers
# ─────────────────────────────────────────────────────────────────────────────

def _completeness_sql(schema, table, null_expr):
    return f"""
        SELECT
            COUNT(*) AS total_rows,
            COALESCE(SUM({null_expr}), 0) AS failed_rows,
            ROUND(100.0 * (1 - COALESCE(SUM({null_expr}), 0) / NULLIF(COUNT(*), 0)), 2)
                AS pass_percentage
        FROM {schema}.{table}
    """


def run_completeness(cur, schema, table, column, date_type=False):
    if date_type:
        null_expr = f"({column} IS NULL OR CAST({column} AS CHAR) = '')"
    else:
        null_expr = f"({column} IS NULL OR {column} = '')"
    cur.execute(_completeness_sql(schema, table, null_expr))
    return cur.fetchone()


def run_multi_column_completeness(cur, schema, table, columns, date_columns=None):
    date_cols = set(date_columns or [])
    parts = [
        f"({c} IS NULL)" if c in date_cols else f"({c} IS NULL OR {c} = '')"
        for c in list(columns) + list(date_cols)
    ]
    null_expr = " OR ".join(parts)  # must be a string, not a list
    cur.execute(_completeness_sql(schema, table, null_expr))
    return cur.fetchone()

# ─────────────────────────────────────────────────────────────────────────────
# Result builder
# ─────────────────────────────────────────────────────────────────────────────

def get_rule_status(pass_pct, threshold):
    if pass_pct >= threshold:
        return 'PASS'
    elif pass_pct >= max(threshold - 5.0, 0.0):
        return 'WARN'
    return 'FAIL'


def build_result(rule_id, run_id, run_date, schema, table, column, metrics, threshold):
    total  = int(metrics['total_rows']  or 0)
    failed = int(metrics['failed_rows'] or 0)
    pct    = float(metrics['pass_percentage'] or 0.0)
    return {
        'run_id':          run_id,
        'run_date':        run_date,
        'triggered_by':    TRIGGERED_BY,
        'run_status':      'COMPLETED',
        'rule_id':         rule_id,
        'schema_name':     schema,
        'table_name':      table,
        'column_name':     column,
        'total_rows':      total,
        'failed_rows':     failed,
        'pass_percentage': pct,
        'rule_status':     get_rule_status(pct, threshold),
        'created_at':      datetime.now(),
        'updated_at':      datetime.now(),
    }

# ─────────────────────────────────────────────────────────────────────────────
# Rule executor (one thread per rule)
# ─────────────────────────────────────────────────────────────────────────────

def execute_rule(rule_id, config, threshold, run_id, run_date):
    """Execute one DQ rule. Returns a list of result dicts (one per table checked)."""
    results    = []
    check_type = config['check_type']
    conn       = get_connection()

    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:

            if check_type == 'completeness':
                for table in _as_list(config['table']):
                    m = run_completeness(cur, SILVER_SCHEMA, table, config['column'])
                    results.append(build_result(rule_id, run_id, run_date, SILVER_SCHEMA,
                                                table, config['column'], m, threshold))

            elif check_type == 'date_completeness':
                for table in _as_list(config['table']):
                    m = run_completeness(cur, SILVER_SCHEMA, table,
                                         config['column'], date_type=True)
                    results.append(build_result(rule_id, run_id, run_date, SILVER_SCHEMA,
                                                table, config['column'], m, threshold))

            elif check_type == 'multi_completeness':
                for table in config['tables']:
                    try:
                        m = run_completeness(cur, SILVER_SCHEMA, table, config['column'])
                        results.append(build_result(rule_id, run_id, run_date, SILVER_SCHEMA,
                                                    table, config['column'], m, threshold))
                    except Exception as exc:
                        log.warning(f"  [{rule_id}] Skipped {table}.{config['column']}: {exc}")

            elif check_type == 'multi_completeness_custom':
                for table, column in config['tables_columns']:
                    try:
                        m = run_completeness(cur, SILVER_SCHEMA, table, column)
                        results.append(build_result(rule_id, run_id, run_date, SILVER_SCHEMA,
                                                    table, column, m, threshold))
                    except Exception as exc:
                        log.warning(f"  [{rule_id}] Skipped {table}.{column}: {exc}")

            elif check_type == 'multi_column_completeness':
                for table in _as_list(config['table']):
                    m = run_multi_column_completeness(cur, SILVER_SCHEMA, table,
                                                       config['columns'],
                                                       config.get('date_columns'))
                    results.append(build_result(rule_id, run_id, run_date, SILVER_SCHEMA,
                                                table, config['column'], m, threshold))

            elif check_type == 'multi_date_per_table':
                for table, column in config['tables_columns']:
                    try:
                        m = run_completeness(cur, SILVER_SCHEMA, table, column, date_type=True)
                        results.append(build_result(rule_id, run_id, run_date, SILVER_SCHEMA,
                                                    table, column, m, threshold))
                    except Exception as exc:
                        log.warning(f"  [{rule_id}] Skipped {table}.{column}: {exc}")

            else:
                log.warning(f"[{rule_id}] Unknown check_type: {check_type}")

    except Exception as exc:
        log.error(f"[{rule_id}] Rule execution failed: {exc}")
        raise
    finally:
        conn.close()

    return results

# ─────────────────────────────────────────────────────────────────────────────
# Result writer
# ─────────────────────────────────────────────────────────────────────────────

def insert_results(results):
    if not results:
        log.info("No results to insert.")
        return
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.executemany(f"""
                INSERT INTO {RESULT_TABLE} (
                    run_id, run_date, triggered_by, run_status,
                    rule_id, schema_name, table_name, column_name,
                    total_rows, failed_rows, pass_percentage, rule_status,
                    created_at, updated_at
                ) VALUES (
                    %(run_id)s,          %(run_date)s,       %(triggered_by)s, %(run_status)s,
                    %(rule_id)s,         %(schema_name)s,    %(table_name)s,   %(column_name)s,
                    %(total_rows)s,      %(failed_rows)s,    %(pass_percentage)s, %(rule_status)s,
                    %(created_at)s,      %(updated_at)s
                )
            """, results)
        conn.commit()
        log.info(f"Inserted {len(results)} rows into {RESULT_TABLE}.")
    except Exception as exc:
        conn.rollback()
        log.error(f"Failed to insert results: {exc}")
        raise
    finally:
        conn.close()

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    run_id   = f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    run_date = datetime.now()

    log.info(f"{'='*60}")
    log.info(f"DQCC Run started  :  {run_id}")
    log.info(f"{'='*60}")

    # Load thresholds from dq_rule (falls back to defaults if table absent)
    db_thresholds = load_rule_thresholds()

    # Build work list — skip rules that require manual/NLP input
    work = []
    for rule_id, config in RULE_CONFIG.items():
        if rule_id in SKIPPED_RULES:
            log.info(f"[{rule_id}] Skipped (requires NLP / derived cohort logic).")
            continue
        threshold = db_thresholds.get(rule_id, config['default_threshold'])
        work.append((rule_id, config, float(threshold)))

    log.info(f"Executing {len(work)} rules  |  workers={MAX_WORKERS}")

    all_results  = []
    failed_rules = []

    with tqdm(total=len(work), desc="DQ Rules", unit="rule", ncols=80) as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(execute_rule, rule_id, cfg, thr, run_id, run_date): rule_id
                for rule_id, cfg, thr in work
            }
            for future in as_completed(futures):
                rule_id = futures[future]
                try:
                    rows = future.result()
                    all_results.extend(rows)
                    pbar.set_postfix(rule=rule_id, status='OK', rows=len(rows))
                except Exception:
                    failed_rules.append(rule_id)
                    pbar.set_postfix(rule=rule_id, status='ERR')
                pbar.update(1)

    insert_results(all_results)

    # ── Summary ──────────────────────────────────────────────────────────────
    pass_n = sum(1 for r in all_results if r['rule_status'] == 'PASS')
    warn_n = sum(1 for r in all_results if r['rule_status'] == 'WARN')
    fail_n = sum(1 for r in all_results if r['rule_status'] == 'FAIL')

    log.info(f"{'='*60}")
    log.info(f"Run complete  :  {run_id}")
    log.info(f"Result rows   :  {len(all_results)}  "
             f"(PASS={pass_n}  WARN={warn_n}  FAIL={fail_n})")
    if failed_rules:
        log.warning(f"Rules with errors  :  {failed_rules}")
    log.info(f"{'='*60}")


if __name__ == '__main__':
    main()
