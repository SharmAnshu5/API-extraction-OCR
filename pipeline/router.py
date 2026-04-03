import logging
import sys

from pipeline import parser_davp, parser_samvad, parser_api

logger = logging.getLogger(__name__)

def extract_pdf_layout(catogery: str, pdf_path: str):
    fmt = catogery.split(".")[-1].upper()
    if fmt == "DAVP":
        return parser_davp.extract_pdf_layout(pdf_path, mapping_path="pipeline/mapping.json")
    if fmt == "SAMVAD":
        return parser_samvad.extract_pdf_layout(pdf_path, mapping_path="pipeline/mapping.json")
    if fmt == "API":
        return parser_api.extract_pdf_layout(pdf_path, mapping_path="pipeline/mapping.json")
    logger.warning("Unknown format. Falling back to DAVP parser. file=%s", pdf_path)
    return parser_davp.extract_pdf_layout(pdf_path, mapping_path="pipeline/mappingp.json")
    