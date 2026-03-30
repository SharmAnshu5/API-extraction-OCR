import os
import time
import logging
import oracledb
from dotenv import load_dotenv

load_dotenv()

from workflow import process_pdf

# ================= PATHS =================

BASE_DIR = os.getcwd()
INPUT_DIR = os.path.join(BASE_DIR, "input")
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")
ERROR_DIR = os.path.join(BASE_DIR, "error")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# ================= DB =================

DB_CONFIG = {
    "user": "auerp",
    "password": "auerp",
    "dsn": "ERPDB"
}

DB_CLIENT_LIB = "C:/instantclient_23_9"
POLL_INTERVAL = 5

# ================= INIT =================

for d in (INPUT_DIR, PROCESSED_DIR, ERROR_DIR, LOG_DIR):
    os.makedirs(d, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "ro_live.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

try:
    oracledb.init_oracle_client(lib_dir=DB_CLIENT_LIB)
except Exception:
    pass


# ================= WATCHER =================

def run_watcher():
    logging.info("==== RO LIVE RUNNER STARTED ====")

    conn = oracledb.connect(**DB_CONFIG)

    while True:
        try:
            files = [
                os.path.join(INPUT_DIR, f)
                for f in os.listdir(INPUT_DIR)
                if f.lower().endswith(".pdf")
            ]

            for pdf_path in files:
                process_pdf(pdf_path, conn)

            time.sleep(POLL_INTERVAL)

        except Exception:
            logging.exception("WATCHER LOOP ERROR")
            time.sleep(10)


if __name__ == "__main__":
    run_watcher()
