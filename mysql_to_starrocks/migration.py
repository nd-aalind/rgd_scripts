#!/usr/bin/env python3
"""
MySQL → StarRocks table migrator

Usage:
  1. Fill in MYSQL_CONFIG and STARROCKS_CONFIG below.
  2. Add table names to TABLES (source MySQL table → target StarRocks table).
  3. python migration.py

Each table is read from MySQL in batches and pushed to StarRocks via Stream Load.
Re-run safe: Stream Load labels are deterministic per (table + batch_number),
so retrying a failed run only re-sends incomplete batches.
"""

import base64
import http.client
import json
import logging
import sys
import urllib.parse
from datetime import date, datetime

import pymysql
import pymysql.cursors

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── 1. MySQL connection ────────────────────────────────────────────────────────

MYSQL_CONFIG = {
    "host":     "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":     3306,
    "user":     "Aalind",
    "password": "A@L1nd@123",
    "database": "rgd_udm_silver",
    "charset":  "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
    "cursorclass": pymysql.cursors.DictCursor,
}


# ── 2. StarRocks connection ───────────────────────────────────────────────────

STARROCKS_CONFIG = {
    "fe_host":  "starrocks-fe.internal",
    "fe_port":  8030,
    "database": "gold",
    "user":     "root",
    "password": "",
}


# ── 3. Tables to migrate ──────────────────────────────────────────────────────
#
# Format: ("mysql_table", "starrocks_table")
# Use the same name for both if they match:
#   ("patients", "patients")
# Or map to a different target name:
#   ("appointments_fn", "appointments")

TABLES = [
    ("patients",      "patients"),
    ("encounters",    "encounters"),
    # add more tables here
]

BATCH_SIZE = 100_000   # rows per Stream Load call


# =============================================================================
# STREAM LOAD
# =============================================================================

def _stream_load(sr_table: str, payload: bytes, label: str) -> dict:
    """
    PUT payload to StarRocks via HTTP Stream Load.
    Follows the HTTP 307 redirect the FE issues to a BE node — the request body
    is preserved because we send a second explicit PUT instead of using urllib
    (which drops the body on redirect).
    """
    fe_host = STARROCKS_CONFIG["fe_host"]
    fe_port = STARROCKS_CONFIG["fe_port"]
    sr_db   = STARROCKS_CONFIG["database"]
    sr_user = STARROCKS_CONFIG["user"]
    sr_pass = STARROCKS_CONFIG["password"]

    auth = base64.b64encode(f"{sr_user}:{sr_pass}".encode()).decode()
    headers = {
        "Authorization":     f"Basic {auth}",
        "label":             label,
        "format":            "json",
        "strip_outer_array": "true",
        "Content-Type":      "text/plain; charset=utf-8",
    }
    path = f"/api/{sr_db}/{sr_table}/_stream_load"

    def _put(host: str, port: int, p: str) -> tuple[int, str, str]:
        conn = http.client.HTTPConnection(host, port, timeout=300)
        try:
            conn.request("PUT", p, body=payload, headers=headers)
            resp = conn.getresponse()
            return resp.status, resp.getheader("Location", ""), resp.read().decode()
        finally:
            conn.close()

    status, location, body = _put(fe_host, fe_port, path)

    if status == 307 and location:
        parsed  = urllib.parse.urlparse(location)
        be_host = parsed.hostname or fe_host
        be_port = parsed.port or 8040
        be_path = parsed.path + (f"?{parsed.query}" if parsed.query else "")
        status, _, body = _put(be_host, be_port, be_path)

    if status != 200:
        raise RuntimeError(f"HTTP {status}: {body[:400]}")

    resp_json = json.loads(body)
    sr_status = resp_json.get("Status", "Unknown")

    if sr_status == "Label Already Exists":
        return resp_json  # idempotent retry — already loaded

    if sr_status not in ("Success", "Publish Timeout"):
        error_url = resp_json.get("ErrorURL", "")
        raise RuntimeError(
            f"Stream Load failed — Status={sr_status}  ErrorURL={error_url}\n{body[:400]}"
        )

    return resp_json


# =============================================================================
# SERIALIZATION
# =============================================================================

def _serial(v):
    """Make MySQL values JSON-serializable."""
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v


def _rows_to_payload(rows: list[dict]) -> bytes:
    return json.dumps(
        [{k: _serial(r[k]) for k in r} for r in rows],
        ensure_ascii=False,
    ).encode("utf-8")


# =============================================================================
# MIGRATE ONE TABLE
# =============================================================================

def migrate_table(mysql_table: str, sr_table: str) -> None:
    log.info("─" * 60)
    log.info("START  %s  →  %s.%s", mysql_table, STARROCKS_CONFIG["database"], sr_table)

    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS cnt FROM `{mysql_table}`")
            total = cur.fetchone()["cnt"]
            log.info("  MySQL rows: %d", total)

            if total == 0:
                log.info("  Empty table — skipping")
                return

            batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
            loaded  = 0

            for batch_num in range(batches):
                offset = batch_num * BATCH_SIZE
                cur.execute(
                    f"SELECT * FROM `{mysql_table}` LIMIT %s OFFSET %s",
                    (BATCH_SIZE, offset),
                )
                rows = cur.fetchall()
                if not rows:
                    break

                payload = _rows_to_payload(rows)
                label   = f"migrate_{mysql_table}_{batch_num:06d}"

                log.info(
                    "  batch %d/%d  offset=%d  rows=%d  bytes=%d  label=%s",
                    batch_num + 1, batches, offset, len(rows), len(payload), label,
                )

                resp = _stream_load(sr_table, payload, label)
                batch_loaded = int(resp.get("NumberLoadedRows", len(rows)))
                loaded += batch_loaded

                log.info(
                    "  batch %d/%d  loaded=%d  filtered=%d  status=%s",
                    batch_num + 1, batches,
                    batch_loaded,
                    int(resp.get("NumberFilteredRows", 0)),
                    resp.get("Status", "?"),
                )

    finally:
        conn.close()

    log.info("DONE   %s  →  %s  |  total loaded: %d", mysql_table, sr_table, loaded)


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    if not TABLES:
        log.error("TABLES list is empty — add at least one table name and re-run.")
        sys.exit(1)

    log.info("MySQL → StarRocks migration")
    log.info("  Source: %s / %s", MYSQL_CONFIG["host"], MYSQL_CONFIG["database"])
    log.info("  Target: %s / %s", STARROCKS_CONFIG["fe_host"], STARROCKS_CONFIG["database"])
    log.info("  Tables: %d  |  batch_size: %d", len(TABLES), BATCH_SIZE)

    failed = []
    for mysql_table, sr_table in TABLES:
        try:
            migrate_table(mysql_table, sr_table)
        except Exception as exc:
            log.exception("FAILED  %s: %s", mysql_table, exc)
            failed.append(mysql_table)

    log.info("=" * 60)
    log.info("Done — %d succeeded, %d failed", len(TABLES) - len(failed), len(failed))
    if failed:
        log.error("Failed tables: %s", failed)
        sys.exit(1)


if __name__ == "__main__":
    main()
