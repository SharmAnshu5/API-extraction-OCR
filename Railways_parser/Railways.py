import base64
import csv
import json
import io
import os
import re
from dotenv import load_dotenv
from openai import OpenAI
from rapidfuzz import fuzz

# ---------------- ENV SETUP ----------------
os.environ.pop("SSL_CERT_FILE", None)
os.environ.pop("REQUESTS_CA_BUNDLE", None)

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("OPENAI_API_KEY not found in .env file")

client = OpenAI(api_key=api_key)

# ---------------- UTILITIES ----------------
def encode_image(file_path):
    with open(file_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

def format_date(date_str):
    if not date_str:
        return ""
    match = re.search(r'(\d{2})[/-](\d{2})[/-](\d{4})', date_str)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return ""

# ---------------- NORMALIZATION ----------------
def normalize(text: str) -> str:
    if not text:
        return ""
    text = text.upper()
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

# ---------------- MASTER CLIENT LOADER ----------------
def load_client_mapping(csv_path: str):
    """
    MASTER_CLIENT_NAME -> MASTER_CLIENT_CODE (authoritative)
    """
    mapping = {}

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            master_name = row.get("MASTER_CLIENT_NAME", "").strip()
            master_code = row.get("MASTER_CLIENT_CODE", "").strip()

            if not master_name or not master_code:
                continue

            key = normalize(master_name)
            mapping[key] = master_code

    return mapping

# ---------------- CLIENT CODE MAPPER ----------------
def map_client(ro_client_name: str, mapping: dict) -> str:
    if not ro_client_name:
        return ""

    ro_clean = normalize(ro_client_name)

    best_score = 0
    best_key = None

    for master_clean in mapping.keys():
        score = fuzz.token_set_ratio(ro_clean, master_clean)
        if score > best_score:
            best_score = score
            best_key = master_clean

    if best_score >= 70 and best_key:
        return mapping[best_key]

    return ""

# ---------------- MAIN PROCESS ----------------
def process_input_folder(input_folder, output_file, client_mapping):

    headers = [
        "FILE_NAME", "GSTIN","CLIENT_CODE", "CLIENT_NAME","CITY_NAME",
        "RO_CLIENT_CODE", "RO_CLIENT_NAME","AGENCY_NAME", "AGENCY_CODE", "AGENCY_CODE_SUBCODE",
        "AD_CAT", "AD_HEIGHT", "AD_WIDTH",
        "RO_NUMBER", "RO_DATE", "KEY_NUMBER", "CATEGORY", "COLOUR",
        "AD_SUBCAT", "PRODUCT", "PACKAGE_NAME",
        "INSERT_DATE", "RO_REMARKS",
        "AD_SIZE", "Executive", "PAGE_PREMIUM", "POSITIONING",
        "RO_RATE", "RO_AMOUNT", "EXTRACTED_TEXT"
    ]

    Fixed_values = {
        "AGENCY_CODE_SUBCODE": "NONE",
        "AD_CAT": "G02",
        "AD_SUBCAT": "GOVT. DISPLAY",
        "PRODUCT": "DISPLAY ADVERTISING",
    }

    Package_mapping = {
        "DEHRADUN": "AU-DDN",
        "AGRA": "AU-AGR",
        "DELHI": "AU-NWD",
        "CHANDIGARH": "AU-CHD",
        "ROHTAK": "AU-RTK",
        "NAINITAL": "AU-NTL",
        "LUCKNOW": "AU-LKO",
        "PRAYAGRAJ": "AU-ALD",
        "JHANSI": "AU-JHA",
        "VARANASI": "AU-VNS",
        "JALANDHAR": "AU-JAL",
        "DHARAMSHALA": "AU-DHM",
        "ALIGARH": "AU-ALG",
        "GORAKHPUR": "AU-GKP",
        "BAREILLY": "AU-BLY",
        "MEERUT": "AU-MRT",
        "MORADABAD": "AU-MBD",
        "KANPUR": "AU-KNP",
        "SHIMLA": "AU-SML",
        "HISAR": "AU-HIS",
        "KARNAL": "AU-KNL",
        "JAMMU": "AU-JMU"
    }

    results = []

    for file_name in os.listdir(input_folder):
        file_path = os.path.join(input_folder, file_name)

        if not file_name.lower().endswith((".pdf", ".jpg", ".jpeg", ".png")):
            continue

        print(f"Processing: {file_name}")

        try:
            prompt = f"""
            You are an expert data extraction engine specialized in Advertising Release Orders (ROs), including Hindi and English documents.
            Your task is to extract structured data from a Release Order (RO) document and return a STRICT JSON output.

----------------------------------------
🧠 CORE CAPABILITIES REQUIRED
----------------------------------------
1. Understand both Hindi and English text.
2. Translate Hindi content into English internally.
3. Identify structured tabular and unstructured data.
4. Apply strict business rules (NO GUESSING).
5. Filter and extract ONLY "AMAR UJALA" newspaper data if multiple newspapers are present.
----------------------------------------
📦 OUTPUT FORMAT (STRICT JSON ONLY)
----------------------------------------
Return ONLY valid JSON with the following fields:
Keys: {', '.join(headers)}
----------------------------------------
📘 FIELD EXTRACTION RULES
----------------------------------------
🔹 RO_NUMBER
- RO_number is a unique identifier for the release order.
🔹 RO_DATE
- Date of the release order
- Format strictly: dd-mm-yyyy
🔹 INSERT_DATE
- Extract publication date or range or Insertion date mentioned in the RO
- Convert to: dd-mm-yyyy
- If range → take the first date only
🔹 CITY_NAME
- Extract from agency address/location
- Usually appears in header or near agency details
🔹 GSTIN
- Extract from the document
- GSTIN is a 15-character alphanumeric code
🔹 PACKAGE_NAME (VERY IMPORTANT)
- Extract city from RO_CLIENT_NAME or client address
- Convert to CITY CODE using mapping from: {json.dumps(Package_mapping, indent=4)}
- If not found → ""
🔹 RO_CLIENT_NAME
- Client name mentioned in the RO
- Often appears near "To" or in the body of the RO
- Dont take any , . or - in the RO_CLIENT_NAME
- Ro_client_name is the client name with railway in it,
- Put the city_name after the ro_client_name with space in between.
🔹 AGENCY_NAME
- Extract from the document
- Name of the advertising agency responsible for the RO
🔹 AGENCY_CODE
🔹 CATEGORY
- Detect based on content:
    Government ads → "Display"
    Tender-related → "Tender" (Most occuring)
    Recruitment → "Recruitment"
🔹 PRODUCT (choose ONE only)
- DISPLAY ADVERTISING
- TENDER
- PUBLIC NOTICES
- AUCTION
- RECRUITMENT
- ADMISSION NOTICE
- ANNOUNCEMENTS
- OTHERS
🔹 AD_SUBCAT (based on CATEGORY)
- Display → GOVT. DISPLAY
- Tender → GOVT.TENDER
- Others → GOVT. DISPLAY
🔹 AD_WIDTH
- width of the advertisement in (cm)
- Extract numeric values in (cm)
- DO NOT calculate if not mentioned → Take AD_SIZE value
🔹 AD_HEIGHT
- height of the advertisement in (cm)
- Extract numeric values in (cm)
- DO NOT calculate, if not mentioned → "1"
🔹 AD_SIZE
- Extract numeric advertising size in (sq cm)
- if not mentioned then calculate AD_SIZE = AD_WIDTH x AD_HEIGHT
🔹 COLOUR
- color of advertisement as mentioned in the RO
- Only if explicitly mentioned:
  "C" for Clolour or "B" for black & white
- Else → "B"
🔹 RO_RATE
- Extract rate per sq cm as mentioned in the RO
- only Numeric value
- DO NOT calculate or guess if not mentioned → "DAVP"
🔹 RO_AMOUNT
- Extract ONLY final amount
🔹 RO_REMARKS
- Extract:
  - Any remarks related to advertisement
🔹 KEY_NUMBER
- unique identifier for the RO other than RO_NUMBER, often mentioned in the document
  
Hardcoded values:
{json.dumps(Fixed_values, indent=4)}
----------------------------------------
🚫 STRICT RULES (VERY IMPORTANT)
----------------------------------------
- DO NOT GUESS any value
- If value not found → return ""
- DO NOT calculate missing values
- DO NOT hallucinate GSTIN, amounts, or rates
- Always return clean English output
----------------------------------------
📄 EXTRACTED_TEXT
----------------------------------------
- Include FULL RAW OCR TEXT exactly as input (after cleaning garbled Unicode)
----------------------------------------
🧪 FINAL VALIDATION BEFORE OUTPUT
----------------------------------------
- Ensure JSON is valid
- Ensure all dates are dd-mm-yyyy
- Ensure no extra fields
- Ensure empty fields are ""
----------------------------------------
OUTPUT ONLY JSON. NO EXPLANATION.
----------------------------------------
"""

            # ---------- PDF ----------
            if file_name.lower().endswith(".pdf"):
                uploaded = client.files.create(
                    file=open(file_path, "rb"),
                    purpose="assistants"
                )

                response = client.responses.create(
                    model="gpt-4.1-mini",
                    input=[{
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": prompt},
                            {"type": "input_file", "file_id": uploaded.id}
                        ]
                    }]
                )

                raw = response.output_text.strip()

            # ---------- IMAGE ----------
            else:
                base64_data = encode_image(file_path)

                response = client.chat.completions.create(
                    model="gpt-4.1-mini",
                    temperature=0,
                    messages=[{
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {"type": "image_url", "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_data}"
                            }}
                        ]
                    }]
                )

                raw = response.choices[0].message.content.strip()

            raw = raw.replace("\n", "").replace("\r", "").strip()

            match = re.search(r"\{.*\}", raw)
            data = json.loads(match.group(0)) if match else {}

            for key in headers:
                data.setdefault(key, "")

            data["FILE_NAME"] = file_name

            # -------- CLIENT CODE MAPPING (FINAL & STRICT) --------
            data["CLIENT_CODE"] = map_client(
                data.get("RO_CLIENT_NAME", ""),
                client_mapping
            )

            # CLIENT_NAME ONLY FROM RO
            data["CLIENT_NAME"] = data.get("RO_CLIENT_NAME", "")

            data["RO_NUMBER"] = re.sub(r"\s+", "", data.get("RO_NUMBER", ""))
            data["RO_DATE"] = format_date(data.get("RO_DATE", ""))
            data["INSERT_DATE"] = format_date(data.get("INSERT_DATE", ""))

            data["PACKAGE_NAME"] = data.get("PACKAGE_NAME", "").upper().strip()
            data["PAGE_PREMIUM"] = "YES" if data.get("POSITIONING", "").strip() else "NO"
            if data["AD_SIZE"] == "":
                data["AD_SIZE"] = int(data.get("AD_WIDTH")) * int(data.get("AD_HEIGHT"))

            results.append(data)

        except Exception as e:
            print(f"❌ Error in {file_name}: {e}")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    csv_file = output_file.replace(".json", ".csv")
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(results)

    print("✅ JSON & CSV saved successfully.")

# ---------------- RUN ----------------
if __name__ == "__main__":
    client_mapping = load_client_mapping("RAILWAY_ONLY.csv")
    process_input_folder("Railways", "output.json", client_mapping)
