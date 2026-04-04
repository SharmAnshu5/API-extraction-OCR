import os
import sys
import re
import json
import logging
from difflib import SequenceMatcher
import pytesseract
import pandas as pd
from pathlib import Path
from datetime import datetime
from pdf2image import convert_from_path
from PyPDF2 import PdfReader
import pdfplumber

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

def _resource_path(relative_path: str) -> str:
    """Get resource path for bundled files"""
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return str(Path(sys._MEIPASS) / relative_path)
    return str(Path(__file__).resolve().parent.parent / relative_path)

def load_master_df():
    """Load client mapping CSV"""
    csv_path = _resource_path("client_code_mapped.csv")
    logger.info(f"Loading master CSV: {csv_path}")
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path, dtype=str).fillna("")
    else:
        logger.warning(f"CSV not found: {csv_path}")
        return pd.DataFrame()

master_df = load_master_df()

client_code_map = {}
client_name_map = {}
if not master_df.empty:
    client_code_map = dict(
        zip(
            master_df["Ro Client Name"].str.strip().str.upper(),
            master_df["MASTER_CLIENT_CODE"]
        )
    )
    client_name_map = dict(
        zip(
            master_df["Ro Client Name"].str.strip().str.upper(),
            master_df["MASTER_CLIENT_NAME"]
        )
    )

def normalize_client_text(text: str) -> str:
    """Normalize client names so common format differences still match."""
    if not text:
        return ""

    n = str(text).upper()
    n = re.sub(r"\[[^\]]*\]", " ", n)
    n = re.sub(r"\bM/O\b", "MINISTRY OF", n)
    n = re.sub(r"\bDEPT\.?\b", "DEPARTMENT", n)
    n = n.replace("&", " AND ")
    n = re.sub(r"\bAND\b", " AND ", n)
    n = re.sub(r"[^A-Z0-9\s]", " ", n)
    n = re.sub(r"\s+", " ", n)
    return n.strip()


def resolve_client_match(ro_client_name: str):
    """Return the best CLIENT_NAME/CLIENT_CODE for a SAMVAD RO client string."""
    if not ro_client_name or master_df.empty:
        return "", ""

    raw_query = re.sub(r"\s+", " ", str(ro_client_name)).strip().upper()
    norm_query = normalize_client_text(ro_client_name)

    exact_code = client_code_map.get(raw_query, "")
    exact_name = client_name_map.get(raw_query, "")
    if exact_code or exact_name:
        return exact_name, exact_code

    best_score = -1.0
    best_name = ""
    best_code = ""

    for _, row in master_df.iterrows():
        for candidate in (row.get("Ro Client Name", ""), row.get("MASTER_CLIENT_NAME", "")):
            cand_norm = normalize_client_text(candidate)
            if not cand_norm:
                continue

            if norm_query == cand_norm or norm_query in cand_norm or cand_norm in norm_query:
                return row.get("MASTER_CLIENT_NAME", ""), row.get("MASTER_CLIENT_CODE", "")

            seq_score = SequenceMatcher(None, norm_query, cand_norm).ratio()
            query_tokens = set(norm_query.split())
            cand_tokens = set(cand_norm.split())
            token_score = len(query_tokens & cand_tokens) / max(len(query_tokens), len(cand_tokens), 1)
            score = max(seq_score, token_score)

            if score > best_score:
                best_score = score
                best_name = row.get("MASTER_CLIENT_NAME", "")
                best_code = row.get("MASTER_CLIENT_CODE", "")

    if best_score >= 0.75:
        return best_name, best_code

    return "", ""

def load_mapping(mapping_path=None):
    if not mapping_path:
        mapping_path = os.path.join(os.path.dirname(__file__), "mapping.json")

    if not os.path.exists(mapping_path):
        return {}

    with open(mapping_path, "r", encoding="utf-8") as f:
        return json.load(f) 

def extract_text_from_pdf(pdf_path):
    """Extract text from PDF using pdfplumber"""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() or ""
        return text
    except Exception as e:
        logger.error(f"Error extracting text from {pdf_path}: {e}")
        return ""

def remove_noise(text: str) -> str:
    """Clean text by removing noise"""
    if not text:
        return ""
    
    t = text
    t = re.sub(r"https?://\S+", " ", t)
    t = re.sub(r"[()]", " ", t)
    t = re.sub(r"\[\u0900-\u097F]+", " ", t)
    t = re.sub(r"[^a-zA-Z0-9\s\-/:]", " ", t)
    t = re.sub(r"\s{2,}", " ", t)
    t = re.sub(r"\n\s*\n", "\n", t)
    return t.strip()

def extract_from_filename(filename: str) -> dict:
    """Extract fields from filename like '40684-4 Size 8x8.pdf'"""
    fields = {
        "FILE_NAME": filename,
        "AGENCY_CODE": "",
        "AGENCY_NAME": "",
        "CLIENT_CODE": "",
        "CLIENT_NAME": "",
        "RO_CLIENT_CODE": "",
        "RO_CLIENT_NAME": "",
        "RO_NUMBER": "",
        "RO_DATE": "",
        "KEY_NUMBER": "",
        "EXECUTIVE_NAME": "",
        "EXECUTIVE_CODE": "",
        "COLOUR": "",
        "AD_CAT": "",
        "AD_SUBCAT": "",
        "PRODUCT": "",
        "BRAND": "",
        "PACKAGE_NAME": "",
        "INSERT_DATE": "",
        "AD_HEIGHT": "",
        "AD_WIDTH": "",
        "AD_SIZE": "",
        "PAGE_PREMIUM": "",
        "RO_AMOUNT": "",
        "RO_RATE": "",
        "BOOKING_CENTER": "",
        "RO_REMARKS": "",
        "EXTRACTED_TEXT": "",
        "POSITIONING": "",
        "AD_DISCOUNT": ""
    }
    
    try:
        name_without_ext = filename.replace(".pdf", "").strip()
        
        # Extract Key Number (before "Size")
        key_match = re.search(r"^([0-9\-]+)\s+Size", name_without_ext)
        if key_match:
            key_num = key_match.group(1).strip()
            fields["KEY_NUMBER"] = key_num
            fields["RO_NUMBER"] = key_num  # Use as RO_NUMBER
        
        # Extract Ad Size (format: WxH)
        size_match = re.search(r"Size\s+(\d+)\s*x\s*(\d+)", name_without_ext, re.IGNORECASE)
        if size_match:
            width = size_match.group(1).strip()
            height = size_match.group(2).strip()
            fields["AD_SIZE"] = f"{width}x{height}"
        else:
            fields["AD_SIZE"] = ""
        
        # Default values for SAMVAD
        fields["AD_CAT"] = "GO2"
        fields["PRODUCT"] = "DISPLAY-MISC"
        fields["EXECUTIVE_NAME"] = "DIRECT CLIENT"
        
        logger.info(f"Extracted from filename: KEY={fields['KEY_NUMBER']}, SIZE={fields['AD_SIZE']}")
        
    except Exception as e:
        logger.warning(f"Error parsing filename {filename}: {e}")
    
    return fields

def extract_fields(text: str) -> dict:
    mapping = load_mapping()
    fields = {
    "AGENCY_CODE": "",
    "AGENCY_NAME": "",
    "CLIENT_CODE": "",
    "CLIENT_NAME": "",
    "RO_CLIENT_CODE": "",
    "RO_CLIENT_NAME": "",
    "RO_NUMBER": "",
    "RO_DATE": "",
    "KEY_NUMBER": "",
    "EXECUTIVE_NAME": "",
    "EXECUTIVE_CODE": "",
    "COLOUR": "",
    "AD_CAT": "",
    "AD_SUBCAT": "",
    "PRODUCT": "",
    "BRAND": "",
    "PACKAGE_NAME": "",
    "INSERT_DATE": "",
    "AD_HEIGHT": "",
    "AD_WIDTH": "",
    "AD_SIZE": "",
    "PAGE_PREMIUM": "",
    "RO_AMOUNT": "",
    "RO_RATE": "",
    "BOOKING_CENTER": "",
    "RO_REMARKS": "",
    "EXTRACTED_TEXT": "",
    "POSITIONING": "",
    "AD_DISCOUNT": ""
    }
    
    agency_match = re.search(r'([A-Z\s\n]{30,})\s+ADVERTISEMENT RELEASE ORDER',text)
    if agency_match:
        agency_block = agency_match.group(1)
        agency_block = agency_block.replace("\n", " ")
        agency_block = re.sub(r"\s+", " ", agency_block).strip()
        # fields["AGENCY_NAME"] = agency_block   
    
    client_match = re.search(r'Dept\.?\s*to\s*which\s*advt\.?\s*relates\s*(.*?)\s*(?:\n|Office|Managing|Director|Under)',
    text,re.IGNORECASE | re.DOTALL)
    if client_match:
        Ro_client_name = client_match.group(1).replace("\n", " ").strip()
        fields["RO_CLIENT_NAME"] = Ro_client_name
        lookup_name = re.sub(r'\s+', ' ', Ro_client_name).strip().upper()
        client_code = client_code_map.get(lookup_name, "")
        client_name = client_name_map.get(lookup_name, "")
        fields["CLIENT_CODE"] = client_code
        fields["CLIENT_NAME"] = client_name
        if not client_code:
            logging.warning(f"CLIENT_CODE not found for: {Ro_client_name}")
        if not client_name:
            logging.warning(f"CLIENT_NAME not found for: {Ro_client_name}")    
    # Pick the first actual RO number in the PDF text instead of the filename fallback.
    # This avoids grabbing the table header "Sr RO No" and keeps the header release-order number.
    ro_matches = re.findall(
        r'RO\s*No\.?\s*[:\-]\s*-?\s*([A-Z0-9/.\-]+)',
        text,
        re.IGNORECASE
    )

    if ro_matches:
        fields["RO_NUMBER"] = re.sub(r'\s+', '', ro_matches[0].strip()).lstrip("-")
    
    edition_matches = re.findall(r'Amar Ujala,\s*([A-Za-z ]+)', text, re.IGNORECASE)
    if edition_matches:
        editions_clean = [e.strip().upper() for e in edition_matches]
        fields["PACKAGE_NAME"] = ", ".join(editions_clean).removesuffix(", DISPLAY").removesuffix(", CLASSIFIED")
        chandigarh_editions = {"ROHTAK DISPLAY", "KARNAL DISPLAY", "HISAR DISPLAY", "KARNAL CLASSIFIED", "ROHTAK CLASSIFIED", "HISAR CLASSIFIED"}
        if any(e in chandigarh_editions for e in editions_clean):
            fields["BOOKING_CENTER"] = "CH0"
        else:
            fields["BOOKING_CENTER"] = "NA1"
    else:
        fields["PACKAGE_NAME"] = ""
        fields["BOOKING_CENTER"] = ""    
    
    subject_match = re.search(r'Subject matter of the advertisement\s*([^\n\r]+)',text,re.IGNORECASE)
    if subject_match:
        fields["CATEGORY"] = subject_match.group(1).strip()               
    code_match = re.search(r'SAMVAD\s*:\-?\s*-?\s*([A-Z0-9/.\-]+)', text, re.IGNORECASE)
    if code_match:
        fields["KEY_NUMBER"] = re.sub(r'\s+', '', code_match.group(1).strip()).lstrip("-").rstrip("/.-")
    elif fields.get("RO_NUMBER"):
        fields["KEY_NUMBER"] = fields["RO_NUMBER"].rsplit("/", 2)[0] if "/" in fields["RO_NUMBER"] else fields["RO_NUMBER"]

    position_match = re.search(
        r'\(Sq\.?\s*cm\)\s*/\s*(Any Page|Front Page)\b',
        text,
        re.IGNORECASE | re.DOTALL
    )
    if not position_match:
        position_match = re.search(r'\b(Any Page|Front Page)\b', text, re.IGNORECASE)
    fields["POSITIONING"] = position_match.group(1).strip() if position_match else ""

    position = fields.get("POSITIONING", "").strip().upper()
    if position in ["ANY PAGE", "FRONT PAGE"]:
        fields["PAGE_PREMIUM"] = "YES"
    else:
        fields["PAGE_PREMIUM"] = "NO"

    ro_number = fields.get("RO_NUMBER", "")
    Ro_client_name = fields.get("RO_CLIENT_NAME", "")
    booking_centre = fields.get("BOOKING_CENTER", "")
    has_C_in_ro = "C" in ro_number.upper()
    is_multi_department = "MULTI DEPARTMENT" in Ro_client_name.upper()

    if has_C_in_ro or is_multi_department:
        if booking_centre == "CHANDIGARH":
            fields["AGENCY_CODE"] = "SA1254SAM190"
            fields["AGENCY_NAME"] = "SAMVAD[M]"
        else:
            fields["AGENCY_CODE"] = "SA1463SAM214"
            fields["AGENCY_NAME"] = "SAMVAD - M"
    else:
        if booking_centre == "CHANDIGARH":
            fields["AGENCY_CODE"] = "SA329SAM81"
            fields["AGENCY_NAME"] = "SAMVAD [DPR HARYANA],PANCHKULA"
        else:
            fields["AGENCY_CODE"] = "SA1462SAM213"
            fields["AGENCY_NAME"] = "SAMVAD - DPR HARYANA"
             
    
    # FIX: Handle None values in packages list
    package_matches = re.findall(r'Amar Ujala,\s*([A-Za-z ]+)', text, re.IGNORECASE)
    if package_matches:
        packages = []
        package_name_map = mapping.get("PACKAGE_NAME_MAP", {})
        for edition in package_matches:
            edition = edition.strip().upper()
            mapped_package = package_name_map.get(edition, edition)
            if mapped_package is not None:
                packages.append(str(mapped_package))
        
        if packages:
            fields["PACKAGE_NAME"] = ", ".join(packages)
        else:
            fields["PACKAGE_NAME"] = ", ".join(package_matches)
    else:
        fields["PACKAGE_NAME"] = ""
      
    if "AGENCY_NAME" in fields and package_matches:
        fields["AGENCY_NAME"] = fields["AGENCY_NAME"] + ", " + package_matches[0]    
    
    Newspaper_Name = ", ".join([f"Amar Ujala, {e}" for e in package_matches])
    remark_match = re.search( r'Remarks?\s*(.*?)\s*B\.\s*Advertisement',text,re.IGNORECASE | re.DOTALL)
    if remark_match:
        remark = remark_match.group(1)
        remark = remark.replace('\n', ' ')
        remark = re.sub(r'\s+', ' ', remark).strip()
        fields["RO_REMARKS"] = Newspaper_Name + ", " + remark.upper() + ", " + fields["AD_SIZE"]
    else:
        fields["RO_REMARKS"] = ""      
        
    pub_date_match = re.search(r"Publication\s*Date.*?(\d{2}-\d{2}-\d{4})",text, re.IGNORECASE | re.DOTALL)
    if pub_date_match:
        fields["INSERT_DATE"] = pub_date_match.group(1)
    else:
        fields["INSERT_DATE"] = ""      
        
    RO_DATE_match = re.search(r"Dated(.*?)From\s*:", text, re.DOTALL)
    if RO_DATE_match:
        section = RO_DATE_match.group(1)
        date_match = re.search(r"\d{2}/\d{2}/\d{4}", section)
        if date_match:
            fields["RO_DATE"] = date_match.group(0)
        else:
            fields["RO_DATE"] = ""
    else:
        fields["RO_DATE"] = ""

    fields["AD_CAT"] = "GO2"
    fields["PRODUCT"] = "DISPLAY - MISCELLANEOUS"
    fields["BRAND"] = "NONE"
    
    category = fields.get("CATEGORY", "")
    mapped_subcat = ""
    category_to_subcat = mapping.get("CATEGORY_TO_SUBCAT", {})
    
    for key in category_to_subcat:
        if key in category:
            mapped_subcat = category_to_subcat.get(key, "")
            break
    
    fields["AD_SUBCAT"] = mapped_subcat  

    color_match = re.search(r'(?:/(Any Page|Front Page))?\s*\n\s*(B&W|Colored)',text,re.IGNORECASE | re.DOTALL)
    if color_match:
        color = color_match.group(2).strip().upper()
        if "B" in color:
            fields["COLOUR"] = "B"
        else:
            fields["COLOUR"] = "C"
    else:
        fields["COLOUR"] = ""

    rate_matches = re.findall(
        r'Rs\.?\s*([\d]+\.\d{2})\s*Rs\.?\s*[\d,]+\.\d{2}',
        text.replace('\n', ' '),
        re.IGNORECASE
    )
    try:
        rates = [float(rate) for rate in rate_matches]
        fields["RO_RATE"] = round(sum(rates), 2) if rates else 0.0
    except (ValueError, TypeError):
        fields["RO_RATE"] = ""
        
    amounts = re.findall(r'Total Cost: Rs\.?\s*([\d,]+\.\d+)', text)
    if amounts:
        final_amount = amounts[-1].replace(",", "")
        fields["RO_AMOUNT"] = float(final_amount)    

    fields["EXTRACTED_TEXT"] = re.sub(r"\s+", " ", text).strip()

    discount_match = re.search( r'and\s*(\d+(?:\.\d+)?)%\s*from\s*Media\s*Houses', text,re.IGNORECASE)
    if discount_match:
        fields["AD_DISCOUNT"] = float(discount_match.group(1))
    else:
        fields["AD_DISCOUNT"] = 0.0
    
    return fields

def extract_pdf_layout(pdf_path, mapping_path=None):
    mapping = load_mapping(mapping_path)
    try:
        logger.info(f"Processing PDF: {pdf_path}")        
        filename = os.path.basename(pdf_path)
        text = extract_text_from_pdf(pdf_path)
        clean_text = remove_noise(text) if text else ""
        
        # Start with filename-based extraction
        fields = extract_from_filename(filename)
        
        # Try to extract from text if available
        if text and len(text.strip()) > 100:  # Only if substantial text
            text_fields = extract_fields(text)
            # Merge text fields, but preserve filename-based RO_NUMBER if text doesn't have one
            for k, v in text_fields.items():
                if not v:
                    continue
                if k == "RO_NUMBER":
                    fields[k] = v
                elif k != "RO_NUMBER" or not fields.get("RO_NUMBER"):
                    fields[k] = v

        # Force both dimensions as per requirement
        fields["AD_HEIGHT"] = "1"
        fields["AD_WIDTH"] = "1"

        fields["EXTRACTED_TEXT"] = clean_text.replace("\n", " ").strip() if clean_text else ""
        
        return text, clean_text, fields    
    except Exception as e:
        logger.exception(f"Error processing {pdf_path}: {e}")
        return "", "", {}
