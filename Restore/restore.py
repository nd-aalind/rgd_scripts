import os
import subprocess
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# ------------------------------------------------------------------
# Logging configuration (ADDED – no existing code modified)
# ------------------------------------------------------------------
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(threadName)s | %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("gcs_mysql_restore.log", mode="a")
    ]
)
logger = logging.getLogger(__name__)

logger.info("Starting GCS → MySQL restore script")

# ------------------------------------------------------------------
# Database connection details
# ------------------------------------------------------------------
DB_HOST     = "localhost"
DB_PORT     = "3306"
DB_USER     = "ndadmin"
DB_PASSWORD = "ndADMIN@2025"
# DB_NAME = "tng-athenone"
# DB_NAME = "deidentified_documents_ocr"
DB_NAME     = "deid_mrg_test"
 
# ------------------------------------------------------------------
# GCS details
# ------------------------------------------------------------------
BUCKET_NAME   = "nd-platform-tng"
FOLDER_PREFIX = "shubhamk_mysql_dump/tng_athena_one"
LOCAL_TEMP_DIR = "/tmp/sql-files"  # Temporary local directory to store a single SQL file


def list_sql_files_in_gcs(bucket_name, folder_prefix):
    """List all .sql files in a specific GCS folder, including nested subfolders."""
    gcs_uri = f"gs://{bucket_name}/{folder_prefix}"
    print(f"Fetching .sql files from {gcs_uri} ...")
    logger.info(f"Listing SQL files from {gcs_uri}")

    result = subprocess.run(
        ["gsutil", "ls", "-r", gcs_uri],
        capture_output=True,
        text=True,
        check=True,
    )

    prefix = f"gs://{bucket_name}/"
    sql_files = [
        line.replace(prefix, "")
        for line in result.stdout.splitlines()
        if line.endswith(".sql")
    ]

    if not sql_files:
        print("No .sql files found in the specified GCS folder.")
        logger.warning("No SQL files found in the specified GCS folder")

    logger.info(f"Total SQL files discovered: {len(sql_files)}")
    return sql_files


def download_sql_file(blob_name, bucket_name, local_dir):
    """Download a single .sql file from GCS to a local directory."""
    os.makedirs(local_dir, exist_ok=True)
    local_file_path = os.path.join(local_dir, os.path.basename(blob_name))
    gcs_uri = f"gs://{bucket_name}/{blob_name}"

    print(f"Downloading {gcs_uri} → {local_file_path}...")
    logger.info(f"Downloading {gcs_uri} → {local_file_path}")

    try:
        subprocess.run(
            ["gsutil", "cp", gcs_uri, local_file_path],
            check=True,
            capture_output=True,
        )
        logger.info(f"Download completed: {local_file_path}")
    except Exception:
        logger.exception(f"Failed to download {gcs_uri}")
        raise

    return local_file_path


def restore_sql_file(sql_file_path):
    """Restore a single .sql file to the Cloud SQL database."""
    print(f"Restoring {sql_file_path} to the database '{DB_NAME}'...")
    logger.info(f"Starting restore for file={sql_file_path}, db={DB_NAME}")

    try:
        command = [
            "mysql",
            f"-h{DB_HOST}",
            f"-P{DB_PORT}",
            f"-u{DB_USER}",
            f"-p{DB_PASSWORD}",
            DB_NAME,
        ]

        with open(sql_file_path, "r") as sql_file:
            process = subprocess.run(
                command,
                stdin=sql_file,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

        if process.returncode == 0:
            print(f"Successfully restored {sql_file_path}.")
            logger.info(f"Restore successful: {sql_file_path}")
        else:
            print(f"Error restoring {sql_file_path}: {process.stderr}")
            logger.error(
                f"Restore failed for {sql_file_path} | "
                f"returncode={process.returncode} | stderr={process.stderr}"
            )

    except Exception as e:
        print(f"Exception occurred while restoring {sql_file_path}: {str(e)}")
        logger.exception(f"Unhandled exception during restore: {sql_file_path}")


def process_sql_file(blob_name):
    """Process a single .sql file: download, restore, and delete."""
    print(f"Processing {blob_name}")
    logger.info(f"Processing blob: {blob_name}")

    try:
        local_file_path = download_sql_file(blob_name, BUCKET_NAME, LOCAL_TEMP_DIR)
        restore_sql_file(local_file_path)

        # os.remove(local_file_path)
        # print(f"Deleted local file: {local_file_path}")
        # logger.info(f"Deleted local file: {local_file_path}")

    except Exception as e:
        print(f"Error processing {blob_name}: {str(e)}")
        logger.exception(f"Failed processing blob: {blob_name}")


def main():
    """Main function to process .sql files one by one."""
    logger.info("Starting main execution")

    sql_files = list_sql_files_in_gcs(BUCKET_NAME, FOLDER_PREFIX)
    print(len(sql_files), sql_files)
    logger.info(f"Submitting {len(sql_files)} files to thread pool")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_sql_file, blob): blob for blob in sql_files}

        for future in as_completed(futures):
            blob = futures[future]
            try:
                future.result()
                logger.info(f"Completed processing for blob: {blob}")
            except Exception as e:
                print(f"Unexpected error while processing {blob}: {str(e)}")
                logger.exception(f"Unexpected error for blob: {blob}")

    print("All files have been processed.")
    logger.info("All files have been processed successfully")


if __name__ == "__main__":
    main()