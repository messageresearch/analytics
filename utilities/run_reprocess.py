import sys
sys.path.append("..")
import csv
import glob
import os
import json
import sys

# Force output flushing
sys.stdout.reconfigure(line_buffering=True)

print("Initing imports...")
import update_sermons
print("Imports done.")

def load_known_speakers():
    speakers_file = "speakers.json"
    if os.path.exists(speakers_file):
        with open(speakers_file, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                return set(data)
            except:
                return set()
    return set()

def reprocess_all_csvs():
    known_speakers = load_known_speakers()
    print(f"Loaded {len(known_speakers)} known speakers.")

    csv_files = glob.glob(os.path.join("data", "*_Summary.csv"))
    total_processed = 0
    total_detected = 0
    total_unknown = 0
    total_fixed_date = 0

    print(f"Found {len(csv_files)} summary files.")

    for i, file_path in enumerate(csv_files):
        filename = os.path.basename(file_path)

        rows = []
        changed_in_file = 0
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                for row in reader:
                    title = row.get('title', '') or ''
                    desc = row.get('description', '') or ''
                    orig_speaker = row.get('speaker', 'Unknown Speaker')
                    
                    if not title.strip():
                        rows.append(row)
                        continue

                    new_speaker = None
                    try:
                        res = update_sermons.identify_speaker_dynamic(title, desc, known_speakers)
                        if res:
                            if isinstance(res, tuple):
                                new_speaker = res[0]
                            else:
                                new_speaker = res
                    except Exception as e:
                        pass
                    
                    final_speaker = new_speaker if new_speaker else "Unknown Speaker"
                    
                    if final_speaker != "Unknown Speaker":
                         total_detected += 1
                    else:
                         total_unknown += 1

                    if final_speaker != orig_speaker:
                        row['speaker'] = final_speaker
                        changed_in_file += 1
                    
                    rows.append(row)
            
            if changed_in_file > 0:
                print(f"  {filename}: Updated {changed_in_file} rows.")
                with open(file_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(rows)

        except Exception as e:
            print(f"Failed to process {file_path}: {e}")

if __name__ == "__main__":
    reprocess_all_csvs()
