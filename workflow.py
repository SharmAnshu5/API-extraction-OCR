import os
import shutil
import logging
from concurrent.futures import ProcessPoolExecutor, TimeoutError as FuturesTimeoutError
from pipeline.identifier import classify_by_filename, classify_pdf
from pipeline.parser_samvad import extract_pdf_layout as extract_samvad
from pipeline.router import extract_pdf_layout
from db.ro_insert import insert_ro_data
from validator import validate_mandatory_fields
from email_service import send_issue_email_adops, run_booking_logic
from db.ro_validation import ro_exists_in_db

logger = logging.getLogger(__name__)

PDF_PARSE_TIMEOUT_SEC = 9

def _extract_worker(pdf_path, doc_type):
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(pdf_path)    
    logger.info(f"Using parser: {doc_type}")    
    try:
        if doc_type == "SAMVAD":
            structured_text, clean_text, fields = extract_samvad(pdf_path)        
        elif doc_type == "DAVP":
            # Pass doc_type FIRST, then pdf_path (router signature is category, pdf_path)
            structured_text, clean_text, fields = extract_pdf_layout(doc_type, pdf_path)        
        else:
            logger.warning(f"Unknown doc_type: {doc_type}. Using DAVP fallback.")
            structured_text, clean_text, fields = extract_pdf_layout("DAVP", pdf_path)    
    except Exception as e:
        logger.exception(f"Extraction error: {e}")
        raise    
    return structured_text, clean_text, fields

def _get_destination_folders(pdf_path):
    parent_dir = os.path.dirname(os.path.dirname(pdf_path))     
    processed_folder = os.path.join(parent_dir, "processed")
    error_folder = os.path.join(parent_dir, "error")    
    return processed_folder, error_folder

def process_pdf(pdf_path, conn, doc_type=None):    
    if not os.path.exists(pdf_path):
        logger.warning("Skip: file not found | %s", pdf_path)
        return False, "FILE_MISSING"
    
    filename = os.path.basename(pdf_path)
    processed_folder, error_folder = _get_destination_folders(pdf_path)
    
    # Auto-detect doc type if not provided
    if not doc_type:
        try:
            # First try filename-based classification
            detected = classify_by_filename(filename)
            if detected and detected != "Others":
                doc_type = detected
            else:
                # Fallback to PDF content classification
                doc_type = classify_pdf(pdf_path)
        except Exception as e:
            logger.warning(f"Classification failed, defaulting to DAVP: {e}")
            doc_type = "DAVP"
    
    logger.info(f"START PROCESSING | DocType={doc_type} | {filename}")
    
    with ProcessPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_extract_worker, pdf_path, doc_type)
        try:
            structured_text, clean_text, fields = fut.result(timeout=PDF_PARSE_TIMEOUT_SEC)
        except FileNotFoundError:
            logger.warning("File disappeared during parse | %s", pdf_path)
            _move_file(pdf_path, error_folder)
            return False, "FILE_MISSING"
        except FuturesTimeoutError:
            logger.error("PDF parse timeout | %s", pdf_path)
            _move_file(pdf_path, error_folder)
            return False, "TIMEOUT"        
        except Exception as e:
            logger.exception(f"PDF extraction failed: {e}")
            _move_file(pdf_path, error_folder)
            return False, "EXTRACTION_ERROR"
    
    if not fields:
        logger.error("No fields extracted from PDF")
        _move_file(pdf_path, error_folder)
        return False, "NO_FIELDS_EXTRACTED"
    
    fields["FILE_NAME"] = filename
    fields["PDF_PATH"] = pdf_path
    fields["STRUCTURED_TEXT"] = structured_text
    fields["CLEAN_TEXT"] = clean_text
    
    logger.debug(f"Extracted fields: {list(fields.keys())}")
    
    missing = validate_mandatory_fields(fields)
    if missing:
        logger.error(f"Validation Failed | Missing: {missing}")
        send_issue_email_adops(
            adbook=fields,
            issue=[f"Missing Field: {m}" for m in missing]
        )
        _move_file(pdf_path, error_folder)
        return False, "VALIDATION_FAILED"
    
    ro_no = fields["RO_NUMBER"]   
    try:
        insert_ro_data(conn, fields)
        logger.info(f"✓ DB INSERT SUCCESS | {ro_no}")
    except Exception as e:
        logger.exception(f"DB Insert Failed: {e}")
        send_issue_email_adops(
            adbook=fields,
            issue=[f"Database Insert Error: {str(e)}"]
        )
        _move_file(pdf_path, error_folder)
        return False, "DB_ERROR"
    
    try:
        booking_success = run_booking_logic(fields, conn)        
        if not booking_success:
            logger.error("Booking Logic Failed")
            send_issue_email_adops(
                adbook=fields,
                issue=["Booking Logic Failed"]
            )
            _move_file(pdf_path, error_folder)
            return False, "BOOKING_FAILED"
    except Exception as e:
        logger.exception(f"Booking Exception: {e}")
        send_issue_email_adops(
            adbook=fields,
            issue=[f"Booking Exception: {str(e)}"]
        )
        _move_file(pdf_path, error_folder)
        return False, "BOOKING_ERROR"    
    
    logger.info(f"✓ SUCCESS | {filename}")
    processed_path = _move_file(pdf_path, processed_folder)
    return True, "SUCCESS"

def _move_file(pdf_path, destination_folder):
    """Move processed file to destination folder"""
    try:
        os.makedirs(destination_folder, exist_ok=True)
        filename = os.path.basename(pdf_path)
        dest_path = os.path.join(destination_folder, filename)
        
        if dest_path.lower().endswith('.pdf.pdf'):
            dest_path = dest_path[:-4]
        
        if os.path.exists(dest_path):
            os.remove(dest_path)
        
        shutil.move(pdf_path, dest_path)
        logger.info(f"Moved to {destination_folder} | {filename}")
        return dest_path
        
    except Exception as e:
        logger.error(f"Failed to move file {pdf_path}: {e}")
        return None

