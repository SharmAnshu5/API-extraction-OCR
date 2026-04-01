import os
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
INPUT_FOLDERS = ["input folder"]
OUTPUT_JSON = "output.json"
OUTPUT_CSV = "output.csv"
EMAIL_SENDER = "your@email.com"
EMAIL_PASSWORD = "yourpassword"
EMAIL_RECEIVER = "admin@email.com"
