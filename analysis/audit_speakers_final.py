import os
import json
import csv
from collections import Counter

DATA_DIR = 'data'
SPEAKERS_FILE = 'speakers.json'

def load_json_file(filepath):
    if not os.path.exists(filepath):
        return set()
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, list):
                return set(data)
            return set()
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return set()

def main():
    # 1. Load known valid speakers
    known_speakers = load_json_file(SPEAKERS_FILE)
    print(f"Loaded {len(known_speakers)} known speakers from {SPEAKERS_FILE}")

    # 2. Iterate CSV and collect speakers
    csv_speakers_counter = Counter()
    total_rows = 0
    csv_files_count = 0
    
    # Normalize comparison (case-insensitive keys for checking, but keep original for reporting)
    known_speakers_lower = {s.casefold() for s in known_speakers}

    print(f"Scanning CSV files in {DATA_DIR}...")

    for filename in os.listdir(DATA_DIR):
        if filename.endswith('_Summary.csv'):
            filepath = os.path.join(DATA_DIR, filename)
            csv_files_count += 1
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        total_rows += 1
                        speaker = row.get('speaker', '').strip()
                        if speaker:
                            csv_speakers_counter[speaker] += 1
            except Exception as e:
                print(f"Error reading {filename}: {e}")

    # 3. Analyze results
    unique_csv_speakers = list(csv_speakers_counter.keys())
    unique_count = len(unique_csv_speakers)
    
    print(f"\n--- AUDIT RESULTS ---")
    print(f"Files Scanned: {csv_files_count}")
    print(f"Total Rows Processed: {total_rows}")
    print(f"Unique Speaker Names Found in CSVs: {unique_count}")
    
    # 4. Accuracy Check
    print(f"\n--- ACCURACY AUDIT ---")
    print("(Names found in CSVs but NOT in speakers.json)")
    
    unknown_in_csv = []
    for speaker in unique_csv_speakers:
        if speaker.casefold() not in known_speakers_lower:
            # Ignore "Unknown Speaker" as it's a valid placeholder
            if speaker.lower() not in ["unknown speaker", "unknown"]:
                unknown_in_csv.append((speaker, csv_speakers_counter[speaker]))
    
    unknown_in_csv.sort(key=lambda x: x[1], reverse=True)
    
    if unknown_in_csv:
        print(f"Found {len(unknown_in_csv)} names in CSVs that are NOT in speakers.json:")
        print(f"{'Count':<8} | {'Speaker Name'}")
        print("-" * 40)
        for name, count in unknown_in_csv:
            print(f"{count:<8} | {name}")
    else:
        print("All speakers in CSVs are present in speakers.json (or are 'Unknown Speaker').")

    # 5. Suspicious Pattern Check (Heuristics)
    print(f"\n--- SUSPICIOUS PATTERN CHECK ---")
    suspicious = []
    for speaker in unique_csv_speakers:
        s_lower = speaker.lower()
        if any(char.isdigit() for char in speaker):
            suspicious.append((speaker, "Contains Digits"))
        elif len(speaker.split()) > 5:
            suspicious.append((speaker, "Unusually Long (>5 words)"))
        elif s_lower.startswith(("the ", "a ", "an ")):
            suspicious.append((speaker, "Starts with Article"))
            
    if suspicious:
        print(f"Found {len(suspicious)} suspicious looking names:")
        for name, reason in suspicious:
            print(f"[{reason}] {name}")
    else:
        print("No obviously malformed names found using basic heuristics.")

if __name__ == "__main__":
    main()
