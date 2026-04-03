import logging
import os
import shutil
import pdfplumber

SAMVAD_KEYWORDS = [
    "SOCIETY FOR ADVANCED MANAGEMENT OF COMMUNICATION",
    "SAMVAD",
    "MD -Cum- CEO"
]

CBC_KEYWORDS = [
    "CENTRAL BUREAU OF COMMUNICATION",
    "Government of India",
    "RO Code",
    "CBC"
]

def classify_pdf(pdf_path):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() or ""

        text = text.upper()

        if any(keyword.upper() in text for keyword in SAMVAD_KEYWORDS):
            return "SAMVAD"

        elif any(keyword.upper() in text for keyword in CBC_KEYWORDS):
            return "DAVP"

        else:
            return "Others"

    except Exception as e:
        print(f"Error reading {pdf_path}: {e}")
        return "Others"


def classify_by_filename(filename):
    name = filename.upper()

    if "CENTRAL BUREAU OF COMMUNICATION" in name or "CBC" in name:
        return "DAVP"

    elif "-" in name and "SIZE" in name:
        return "SAMVAD"

    else:
        return "Others"

def classify_RO_catogory(SOURCE_FOLDER):
    for filename in os.listdir(SOURCE_FOLDER):
        if filename.lower().endswith(".pdf"):
            file_path = os.path.join(SOURCE_FOLDER, filename)
            category = classify_pdf(file_path)
            
            if category == "Others":
                logging.info(f"Classified as Others so classifying by checking the Content of file name {filename}")
                category = classify_by_filename(filename)
                logging.info(f"Classified as {category} based on filename: {filename}")
                if category == "Others":
                     logging.warning(f"Could not classify {filename} based on content or filename. Defaulting to Others.")   
                else:
                    return category
            else:
                return category                    
            print(f"{filename} → {category}")
    
    logging.info("Classification Completed!")
