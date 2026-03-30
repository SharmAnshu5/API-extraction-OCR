import time
import os
import shutil
import logging
import oracledb

from parser import parse_fields
from ocr.ocr_engine import extract_ocr_text
from db.ro_insert import insert_ro_data

INPUT_DIR = "input"
PROCESSED_DIR = "processed"
ERROR_DIR = "error"

logging.basicConfig(
    filename="logs/ro_live.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def process_file(pdf_file, conn):
    try:
        pdf_path = os.path.join(INPUT_DIR, pdf_file)

        ocr_text = extract_ocr_text(pdf_path)
        if not ocr_text or len(ocr_text) < 100:
            raise ValueError("OCR text too small")

        data = parse_fields(ocr_text, pdf_path)

        if not data.get("RO_NUMBER") or not data.get("CLIENT_NAME"):
            raise ValueError("Mandatory fields missing")

        insert_ro_data(conn, data)

        shutil.move(pdf_path, os.path.join(PROCESSED_DIR, pdf_file))
        logging.info(f"SUCCESS | {pdf_file}")

    except Exception as e:
        shutil.move(pdf_path, os.path.join(ERROR_DIR, pdf_file))
        logging.error(f"FAILED | {pdf_file} | {str(e)}")

def watch_folder():
    conn = oracledb.connect(user="XXX", password="XXX", dsn="XXX")

    while True:
        files = [f for f in os.listdir(INPUT_DIR) if f.lower().endswith(".pdf")]

        for f in files:
            process_file(f, conn)

        time.sleep(5)

if __name__ == "__main__":
    watch_folder()
