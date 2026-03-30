import os
import shutil
import logging

from parser import extract_pdf_layout
from db.ro_insert import insert_ro_data
from validator import validate_mandatory_fields
from email_service import send_issue_email_adops, send_booking_mail, run_booking_logic  # adjust if needed
from db.ro_validation import ro_exists_in_db

logger = logging.getLogger("workflow")

ERROR_FOLDER = "error"
PROCESSED_FOLDER = "processed"


def move_to_error_folder(pdf_path):
    os.makedirs(ERROR_FOLDER, exist_ok=True)
    shutil.move(pdf_path, os.path.join(ERROR_FOLDER, os.path.basename(pdf_path)))


def move_to_processed_folder(pdf_path):
    os.makedirs(PROCESSED_FOLDER, exist_ok=True)
    shutil.move(pdf_path, os.path.join(PROCESSED_FOLDER, os.path.basename(pdf_path)))


def process_pdf(pdf_path, conn):

    filename = os.path.basename(pdf_path)
    logger.info(f"START PROCESSING | {filename}")

    # =======================
    # EXTRACTION
    # =======================

    try:
        structured_text, clean_text, fields = extract_pdf_layout(pdf_path)
        fields["PDF_PATH"] = pdf_path

    except Exception:
        logger.exception("Extraction Failed")

        send_issue_email_adops(
            adbook={"RO_NO": "UNKNOWN", "PDF_PATH": pdf_path},
            issue=["Extraction Error"]
        )

        move_to_error_folder(pdf_path)
        return

    # =======================
    # VALIDATION - MANDATORY
    # =======================

    missing = validate_mandatory_fields(fields)

    if missing:
        logger.error(f"Validation Failed | Missing: {missing}")

        send_issue_email_adops(
            adbook=fields,
            issue=[f"Missing Field: {m}" for m in missing]
        )

        move_to_error_folder(pdf_path)
        return

    # =======================
    # VALIDATION - DUPLICATE RO
    # =======================

    ro_no = fields.get("RO_NO")

    if ro_exists_in_db(conn, ro_no):
        logger.error(f"Duplicate RO Detected: {ro_no}")

        send_issue_email_adops(
            adbook=fields,
            issue=[f"Duplicate RO Number: {ro_no}"]
        )

        move_to_error_folder(pdf_path)
        return

    # =======================
    # DB INSERT
    # =======================

    try:
        insert_ro_data(conn, fields)
        logger.info("DB Insert Success")

    except Exception:
        logger.exception("DB Insert Failed")

        send_issue_email_adops(
            adbook=fields,
            issue=["Database Insert Error"]
        )

        move_to_error_folder(pdf_path)
        return

    # =======================
    # BOOKING
    # =======================

    try:
        booking_success = run_booking_logic(fields, conn)
    except Exception:
        logger.exception("Booking Logic Crash")

        send_issue_email_adops(
            adbook=fields,
            issue=["Booking Exception"]
        )
        return

    if not booking_success:
        logger.error("Booking Failed")

        send_issue_email_adops(
            adbook=fields,
            issue=["Booking Error"]
        )
        return

    # =======================
    # SUCCESS
    # =======================

    send_booking_mail(fields)
    move_to_processed_folder(pdf_path)

    logger.info(f"SUCCESS | {filename}")

