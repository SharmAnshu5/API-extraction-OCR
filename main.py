import os
import time
import logging
import oracledb
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from workflow import process_pdf

load_dotenv()

# ================= MULTIPLE LOCATIONS =================
LOCATIONS = [
    {"base": r"Y:\NA1", "name": "NA1"},
    {"base": r"Y:\CH0", "name": "CH0"},
]
POLL_INTERVAL = 1

# ================= DB =================
user = os.getenv("db_user")
password = os.getenv("db_password")
dsn = os.getenv("db_dsn")
tns_admin = os.getenv("TNS_ADMIN")
DB_CLIENT_LIB = os.getenv("DB_CLIENT_LIB")

if not all([user, password, dsn]):
    logging.error(
        "MISSING DB CREDENTIALS | user=%s password=%s dsn=%s",
        "SET" if user else "MISSING",
        "SET" if password else "MISSING",
        "SET" if dsn else "MISSING",
    )
    raise ValueError("Database credentials incomplete. Check .env file.")

if tns_admin:
    os.environ["TNS_ADMIN"] = tns_admin

DB_CONFIG = {"user": user, "password": password, "dsn": dsn}

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

try:
    oracledb.init_oracle_client(lib_dir=DB_CLIENT_LIB)
except Exception:
    pass

_IN_PROGRESS = set()


def setup_location_dirs(base_dir: str):
    dirs = {
        "input": os.path.join(base_dir, "input"),
        "processed": os.path.join(base_dir, "processed"),
        "error": os.path.join(base_dir, "error"),
        "logs": os.path.join(base_dir, "logs"),
    }
    for d in dirs.values():
        os.makedirs(d, exist_ok=True)
    return dirs


def wait_until_stable(file_path, checks=3, delay=1):
    last = -1
    same = 0
    for _ in range(20):
        if not os.path.exists(file_path):
            return False
        size = os.path.getsize(file_path)
        if size == last:
            same += 1
            if same >= checks:
                return True
        else:
            same = 0
            last = size
        time.sleep(delay)
    return False


def is_connection_alive(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM DUAL")
        cur.close()
        return True
    except Exception:
        return False


class PDFHandler(FileSystemEventHandler):
    def __init__(self, conn_factory, location_name):
        self.conn_factory = conn_factory
        self.location_name = location_name

    def on_created(self, event):
        if event.is_directory:
            return

        file_path = event.src_path
        if not file_path.lower().endswith(".pdf"):
            return

        if file_path in _IN_PROGRESS:
            logging.info("[%s] Skip duplicate event | %s", self.location_name, file_path)
            return

        _IN_PROGRESS.add(file_path)
        try:
            if not wait_until_stable(file_path):
                logging.warning("[%s] Skip unstable/missing file | %s", self.location_name, file_path)
                return
            conn = self.conn_factory()
            try:
                process_pdf(file_path, conn)
            finally:
                conn.close()
        except Exception:
            logging.exception("[%s] FILE PROCESSING FAILED | %s", self.location_name, file_path)
        finally:
            _IN_PROGRESS.discard(file_path)


def run_watcher():
    logging.info("==== RO LIVE RUNNER STARTED (MULTI-LOCATION) ====")
    observers = []
    location_dirs = {}

    def get_fresh_connection():
        return oracledb.connect(**DB_CONFIG)

    try:
        test_conn = get_fresh_connection()
        test_conn.close()
        logging.info("Database connection test successful.")

        for loc in LOCATIONS:
            base = loc["base"]
            name = loc["name"]

            if not os.path.exists(base):
                logging.warning("Location not found: %s", base)
                continue

            dirs = setup_location_dirs(base)
            location_dirs[name] = dirs
            logging.info("Initialized location: %s | %s", name, base)

            event_handler = PDFHandler(get_fresh_connection, name)
            observer = Observer()
            observer.schedule(event_handler, dirs["input"], recursive=False)
            observer.start()
            observers.append(observer)
            logging.info("Watcher started for %s on: %s", name, dirs["input"])

        logging.info("==== ALL WATCHERS ACTIVE ====")

        while True:
            conn = None
            try:
                conn = get_fresh_connection()

                for name, dirs in location_dirs.items():
                    input_dir = dirs["input"]
                    files = [
                        os.path.join(input_dir, f)
                        for f in os.listdir(input_dir)
                        if f.lower().endswith(".pdf")
                    ]

                    if files:
                        logging.info("[%s] Polling found %d PDF(s).", name, len(files))

                    for pdf_path in files:
                        if pdf_path in _IN_PROGRESS:
                            continue
                        _IN_PROGRESS.add(pdf_path)
                        try:
                            if not is_connection_alive(conn):
                                logging.warning("Connection lost, reconnecting...")
                                conn.close()
                                conn = get_fresh_connection()

                            if wait_until_stable(pdf_path):
                                process_pdf(pdf_path, conn)
                                logging.info("[%s] Processed: %s", name, pdf_path)
                        except Exception:
                            logging.exception("[%s] Processing failed for: %s", name, pdf_path)
                        finally:
                            _IN_PROGRESS.discard(pdf_path)

                time.sleep(POLL_INTERVAL)

            except Exception:
                logging.exception("WATCHER LOOP ERROR")
                time.sleep(10)
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

    except KeyboardInterrupt:
        logging.info("Shutdown requested by user (Ctrl+C).")
    except Exception:
        logging.exception("Fatal error in watcher startup/runtime.")
    finally:
        for observer in observers:
            try:
                observer.stop()
                observer.join()
            except Exception:
                logging.exception("Failed stopping watcher cleanly.")
        logging.info("==== RO LIVE RUNNER STOPPED ====")


if __name__ == "__main__":
    run_watcher()
