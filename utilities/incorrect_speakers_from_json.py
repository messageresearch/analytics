import json
import re

# Load speakers.json
with open('speakers.json', 'r', encoding='utf-8') as f:
    speakers = json.load(f)

# Patterns and keywords to flag as incorrect speakers
days_of_week = {"sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"}
event_keywords = [
    "communion", "service", "meeting", "youth", "camp", "combined", "school", "choir", "skit", "wedding", "special", "pm", "am", "night", "morning", "evening", "testimony", "report", "banquet", "anniversary", "conference", "song", "tape", "broadcast", "memorial", "tribute", "funeral", "graduation", "easter", "christmas", "sabbath"
]
parsing_errors = [".timestamped", "de la", "las siete edades", "flag tab", "spoken word", "robson brothers", "fmt choir"]

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

incorrect = [s for s in speakers if is_incorrect_speaker(s)]

with open('incorrect_speakers_from_json.csv', 'w', encoding='utf-8', newline='') as f:
    for name in incorrect:
        f.write(f'{name}\n')

print(f"Wrote {len(incorrect)} incorrect speaker names to incorrect_speakers_from_json.csv")
