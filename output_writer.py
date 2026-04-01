import json, csv, os
from config import OUTPUT_JSON, OUTPUT_CSV


def save_output(data_list):
    os.makedirs("output", exist_ok=True)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(data_list, f, indent=4)

    if data_list:
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data_list[0].keys())
            writer.writeheader()
            writer.writerows(data_list)