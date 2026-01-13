import os
import subprocess
import logging
from datetime import datetime, timedelta

# Configuration
PGPASSFILE = "/home/cdpuser/scripts/daily-cm-db-backup/.pgpass"
BACKUP_DIR = "/home/cdpuser/scripts/daily-cm-db-backup/backup_psql"
LOG_FILE = os.path.join(BACKUP_DIR, "backup.log")
DAYS_TO_KEEP = 5
FILE_SUFFIX = "pg_backup.sql.gz"
DATABASE = "metastore"
USER = "hive"
HOST = "10.11.229.10"  # Updated PostgreSQL server address

# Setup logging
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(message)s")

def log(message):
    """Logs a message with timestamp."""
    print(message)
    logging.info(message)

# Ensure backup directory exists
if not os.path.isdir(BACKUP_DIR):
    log(f"ERROR: Backup directory {BACKUP_DIR} does not exist. Exiting.")
    exit(1)

# Change to backup directory
try:
    os.chdir(BACKUP_DIR)
except Exception as e:
    log(f"ERROR: Unable to change to backup directory {BACKUP_DIR}. Exiting. {str(e)}")
    exit(1)

# Create a timestamped backup file
timestamp = datetime.now().strftime("%Y%m%d%H%M")
output_file = os.path.join(BACKUP_DIR, f"{timestamp}_pg_backup.sql")

log(f"💾 Starting PostgreSQL backup for database: {DATABASE} on {HOST}")

# Run pg_dump command
backup_command = ["pg_dump", "-h", HOST, "-U", USER, DATABASE, "-F", "p", "-f", output_file]
env = os.environ.copy()
env["PGPASSFILE"] = PGPASSFILE  # Set the password file

try:
    subprocess.run(backup_command, check=True, env=env)
    log(f"✅ Database backup completed successfully. File: {output_file}")
except subprocess.CalledProcessError:
    log("❌ ERROR: Database backup failed. Exiting.")
    exit(1)

# Compress the backup file
log(f"📦 Compressing backup file: {output_file}")

try:
    subprocess.run(["gzip", output_file], check=True)
    log(f"✅ Compression successful: {output_file}.gz")
except subprocess.CalledProcessError:
    log("❌ ERROR: Failed to compress backup file. Exiting.")
    exit(1)

# Cleanup old backups
log(f"🧹 Pruning backups older than {DAYS_TO_KEEP} days.")

cutoff_date = datetime.now() - timedelta(days=DAYS_TO_KEEP)

for file in os.listdir(BACKUP_DIR):
    file_path = os.path.join(BACKUP_DIR, file)
    if file.endswith(FILE_SUFFIX):
        file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        if file_time < cutoff_date:
            try:
                os.remove(file_path)
                log(f"✅ Deleted old backup: {file_path}")
            except Exception as e:
                log(f"❌ ERROR: Failed to delete {file_path}. {str(e)}")

log("🎉 Backup process completed successfully. See you tomorrow!")

