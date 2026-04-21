import base64
import csv
import json
import io
import os
import re
from dotenv import load_dotenv
from openai import OpenAI
from rapidfuzz import process, fuzz

# Load API key
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("OPENAI_API_KEY not found in .env file")

client = OpenAI(api_key=api_key)

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

def load_client_mapping(csv_path):
    mapping = {}

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = normalize(row["MASTER_CLIENT_NAME"])
            mapping[key] = {
                "CLIENT_CODE": row.get("CLIENT_CODE", "").strip(),
                "CLIENT_NAME": row.get("CLIENT_NAME", "").strip()
            }

    return mapping

def normalize(text):
    return re.sub(r'[^A-Z0-9 ]', '', text.upper()).strip()

def map_client(ro_client_name, mapping):
    if not ro_client_name:
        return "", ""

    ro_clean = normalize(ro_client_name)
    choices = list(mapping.keys())

    result = process.extractOne(
    ro_clean,
    choices,
    scorer=fuzz.partial_ratio
)
    if result:
        match, score, _ = result
        if score > 75:  # Threshold for matching
            return mapping[match]["CLIENT_CODE"], mapping[match]["CLIENT_NAME"]

    return "", ""

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
- RO_CLIENT_NAME SHOULD ALSO CONTAIN PACKAGE_NAME WITHOUT MAPPING
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
- if not mentioned → calculate as AD_WIDTH x AD_HEIGHT (only if both are present)
🔹 COLOUR
- color of advertisement as mentioned in the RO
- Only if explicitly mentioned:
  "C" for Clolour or "B" for black & white
- Else → ""
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
            # -------- PDF --------
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

            # -------- IMAGE --------
            else:
                base64_data = encode_image(file_path)

                response = client.chat.completions.create(
                    model="gpt-4.1-mini",
                    temperature=0,
                    messages=[{
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{base64_data}"
                                }
                            }
                        ]
                    }]
                )

                raw = response.choices[0].message.content.strip()

            # CLEAN JSON TEXT
            raw = raw.replace("\n", "").replace("\r", "").strip()

            def extract_json(text):
                if not text:
                    return "{}"
                match = re.search(r"\{.*\}", text, re.DOTALL)
                if match:
                    return match.group(0)
                return "{}"

            clean_json = extract_json(raw)
            try:
                data = json.loads(clean_json)
            except:
                data = {}

            # Ensure all keys exist
            for key in headers:
                data.setdefault(key, "")

            # FILE_NAME
            data["FILE_NAME"] = file_name
            
            # CLIENT MAPPING
            client_code, client_name = map_client(data.get("RO_CLIENT_NAME", ""), client_mapping)
            data["CLIENT_CODE"] = client_code
            data["CLIENT_NAME"] = client_name

            # CLEAN RO_NUMBER
            data["RO_NUMBER"] = re.sub(r"\s+", "", data.get("RO_NUMBER", ""))

            # FORMAT DATES
            data["RO_DATE"] = format_date(data.get("RO_DATE", ""))
            data["INSERT_DATE"] = format_date(data.get("INSERT_DATE", ""))

            # PACKAGE_NAME uppercase
            data["PACKAGE_NAME"] = data.get("PACKAGE_NAME", "").upper().strip()

            # PAGE_PREMIUM logic
            pos = data.get("POSITIONING", "").strip()
            data["PAGE_PREMIUM"] = "YES" if pos else "NO"

            results.append(data)

        except Exception as e:
            print(f"❌ Error in {file_name}: {e}")
    
        # SAVE JSON
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    print("✅ JSON saved.")

    # SAVE CSV
    csv_file = output_file.replace(".json", ".csv")

    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)

        writer.writeheader()
        writer.writerows(results)

    print("✅ CSV saved.")


# RUN
if __name__ == "__main__":
    client_mapping = load_client_mapping("ALL_DATA.csv") # Load client mapping from CSV
    process_input_folder(r"Railway RO's", r"output.json", client_mapping) # input_folder_contains_pdf, output_json_file
