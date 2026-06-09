import os
from datetime import datetime

# Load .env when available (optional dependency)
# Load from cwd first, then from project root so running from any subdir (e.g. pyspark_qc) still picks up root .env
try:
    from dotenv import load_dotenv
    load_dotenv()
    _config_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(_config_dir, "..", ".env"))
except ImportError:
    pass

# Database Configuration (from environment variables)
DB_CONFIG = {
    "user": os.getenv("DB_USER", "nd-root-mysql"),
    "password": os.getenv("DB_PASSWORD", "kmsamd89undsd4"),
    "host": os.getenv("DB_HOST", "172.16.2.42"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "database": os.getenv("DB_NAME", "rgd_udm_silver")
}


# TABLE_LIST = [
#    "patient_demographics","encounters_01282026","diagnosis_v1","procedures","medications","labs_final","radiology","vitals_29012026","allergies","examinations_01282026","ros_01282026","notes","notes_ao","notes_ao_v1","notes_ao_v2"
# ]

TABLE_LIST = [ # "patient_demographics","allergies_inc"
                "allergies","diagnosis","encounters","examination","labs","procedures","radiology","ros","vitals"
]


# Optional: compute QC stats per value of this column (e.g. psid). Used by pyspark_qc when --by is not set.
# Set to None for full-table stats only. Env QC_GROUP_BY_COLUMN overrides this.
GROUP_BY_COLUMN = os.getenv("QC_GROUP_BY_COLUMN", "psid")  # e.g. "psid"

# Optional: force keyset pagination column per table (speeds up large table reads).
# Use when the table has no single-column PK but has an indexed column (e.g. id, allergy_id).
# Example: {"allergies": "allergy_id", "notes": "note_id"}
TABLE_KEYSET_COLUMNS = {
    "patient_demographics": "ndid",
    "allergies_inc": "ndid",
    "encounters": "ndid",
    "diagnosis_inc": "ndid",
    "radiology_inc": "ndid",
    "examination_inc": "ndid",    
    # "encounters_01282026": "eid",
    # "medications_v1": "psid",
    # "temp_procedures_with_incremental_id": "incremental_id",
}

# Directory Configuration
OUTPUT_DIR = "exports"

STATS_FILE = os.path.join(OUTPUT_DIR, "stats_db.csv")


# Export Configuration
MAX_WORKERS = 5
SAMPLE_SIZE = 50 
CSV_DELIMITER = ","

FILE_EXISTS_ACTION = "append"

# Logging Configuration
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = f"export_logs/mysql_export_{timestamp}.log"
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

