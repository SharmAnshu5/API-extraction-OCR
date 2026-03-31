import base64
import csv
import json
import io
import os
import re
from dotenv import load_dotenv
from openai import OpenAI

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


def process_input_folder(input_folder, output_file):

    headers = [
        "FILE_NAME", "AGENCY_NAME", "GSTIN", "AGENCY_CODE", "Agency_code_subcode",
        "CLIENT_CODE", "CLIENT_NAME","CITY_NAME", "RO_CLIENT_CODE", "RO_CLIENT_NAME",
        "RO_NUMBER", "RO_DATE", "KEY_NUMBER", "CATEGORY", "COLOUR",
        "AD_CAT", "AD_SUBCAT", "PRODUCT", "BRAND", "PACKAGE_NAME",
        "INSERT_DATE", "RO_REMARKS", "AD_HEIGHT", "AD_WIDTH",
        "AD_SIZE", "Executive", "PAGE_PREMIUM", "POSITIONING",
        "RO_RATE", "RO_AMOUNT", "EXTRACTED_TEXT"
    ]

    results = []

    for file_name in os.listdir(input_folder):
        file_path = os.path.join(input_folder, file_name)

        if not file_name.lower().endswith((".pdf", ".jpg", ".jpeg", ".png")):
            continue

        print(f"Processing: {file_name}")

        try:
            prompt = f"""

Extract data from this Release Order.
Return ONLY ONE JSON object (no explanation).

Keys: {', '.join(headers)}

STRICT BUSINESS RULES:
- RO_REMARKS = Caption / Remarks of advertisement / additional information related to advertisement
- AD_SIZE = multiplication of height and width
- RO_RATE = Rate per page
- COLOUR = C or B only
- RO_AMOUNT = Final total amount
- INSERT_DATE = dd-mm-yyyy (it is a date of publication)
- RO_DATE = dd-mm-yyyy (it is a date of release order)
- CITY_NAME = AGENCY NAME city.
- RO_CLIENT_NAME = CLIENT NAME WITHOUT CITY NAME
- PACKAGE_NAME = City name of CLIENT_NAME/Advertisement/Edition but the name should be mapped to a city code using below mapping.
        City Name            : City Code
        "DEHRADUN"           : "AU-DDN",
        "AGRA"               : "AU-AGR",
        "DELHI"              : "AU-NWD",
        "CHANDIGARH"         : "AU-CHA",
        "ROHTAK "            : "AU-RTK",    
        "NAINITAL"           : "AU-NAI",
        "LUCKNOW"            : "AU-LKO",
        "PRAYAGRAJ"          : "AU-ALD",
        "JHANSI"             : "AU-JHA",
        "VARANASI"           : "AU-VNS",
        "JALANDHAR"          : "AU-JAL",
        "DHARAMSHALA"        : "AU-DSL",
        "ALIGARH"            : "AU-ALG",
        "GORAKHPUR"          : "AU-GKP",
        "BAREILLY"           : "AU-BLY",
        "MEERUT"             : "AU-MRT",
        "MORADABAD"          : "AU-MBD",
        "KANPUR"             : "AU-KNP",
        "SHIMLA"             : "AU-SML",
        "HISAR"              : "AU-HIS",
        "KARNAL"             : "AU-KNL",
        "JAMMU"              : "AU-JMU",

product name is to be identified that suits the best from:

DISPLAY ADVERTISING, TENDER, PUBLIC NOTICES, AUCTION, RECRUITMENT, OTHERS, ADMISSION NOTICE, ANNOUNCEMENTS

(Fixed value)AD_CAT = G02
(Fixed value)AGENCY_CODE = NONE
(Fixed value)RO_CLIENT_CODE = NONE

AD_SUBCAT will be determine based on the CATEGORY value using below mapping.
    CATEGORY    :    AD_SUBCAT
    "Display": "GOVT. DISPLAY",
    "Tender": "GOVT.TENDER",
    "Public Notices": "GOVT. DISPLAY",
    "Auction": "GOVT.TENDER",
    "Recruitment": "GOVT. DISPLAY",
    "Others": "GOVT. DISPLAY",
    "Admission Notice": "GOVT. DISPLAY",
    "Announcements": "GOVT. DISPLAY",

If there is C in RO number or MULTI DEPARTMENT in client name then: if booking_centre = CHANDIGARH then Agency_code_subcode = SA1254SAM190 else Agency_code_subcode = SA1463SAM214
else: if booking_centre = CHANDIGARH then Agency_code_subcode = SA329SAM81 else Agency_code_subcode = SA1462SAM213

EXTRACTED_TEXT = The complete extracted text from the RO which can be used for reference. This should be the raw text extracted without any processing.

DO NOT GUESS. If not found → ""
"""

            # -------- PDF --------
            if file_name.lower().endswith(".pdf"):
                uploaded = client.files.create(
                    file=open(file_path, "rb"),
                    purpose="assistants"
                )

                response = client.responses.create(
                    model="gpt-4.1",
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
                    model="gpt-4.1",
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
    process_input_folder(r"input folder", r"output.json") # input_folder_contains_pdf, output_json_file
