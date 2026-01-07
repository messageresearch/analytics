import csv
import glob
import os
import re

# Patterns and keywords to flag as incorrect speakers
days_of_week = {"sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"}
event_keywords = [
    "communion", "service", "meeting", "youth", "camp", "combined", "school", "choir", "skit", "wedding", "special", "pm", "am", "night", "morning", "evening", "testimony", "report", "banquet", "anniversary", "conference", "song", "tape", "broadcast", "memorial", "tribute", "funeral", "graduation", "easter", "christmas", "sabbath", "sunday", "friday", "saturday", "wednesday", "thursday", "monday", "tuesday"
]
parsing_errors = ["de la", "las siete edades", "flag tab", "spoken word", "robson brothers", "fmt choir"]

# Helper to flag suspicious speaker names
def is_incorrect_speaker(speaker):
    if not speaker or speaker.strip() == "":
        return True
    s = speaker.lower().strip()
    if s in days_of_week:
        return True
    if any(kw in s for kw in event_keywords):
        return True
    if any(err in s for err in parsing_errors):
        return True
    if re.match(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", s):
        return True
    if re.match(r"\d{4}-\d{2}-\d{2}", s):
        return True
    if re.match(r"[a-zA-Z]+ [0-9]{1,2}, [0-9]{4}", s):
        return True
    if len(s) < 3:
        return True
    return False

glob_pattern = os.path.join('data', '*_Summary.csv')
summary_files = glob.glob(glob_pattern)

output_rows = []
header_written = False

for csv_path in summary_files:
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames + ["source_file"]
        for row in reader:
            speaker = row.get('speaker', '')
            if is_incorrect_speaker(speaker):
                row["source_file"] = os.path.basename(csv_path)
                output_rows.append(row)

with open('incorrect_speakers_full_rows.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()
    writer.writerows(output_rows)

print(f"Wrote {len(output_rows)} rows with incorrect speakers to incorrect_speakers_full_rows.csv")
