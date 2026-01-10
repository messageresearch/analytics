import csv
import os
import glob
import re

DATA_DIR = "data"
INVALID_TERMS_FILE = "config/speakers_config.json"

# Basic regex for potential names: "Bro. First Last" or "Brother First Last" or "Pastor First Last"
POTENTIAL_NAME_REGEX = re.compile(
    r'\b(?:Bro\.?|Brother|Pastor|Minister|Bishop|Rev\.?|Reverend|Elder|Evangelist)\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)+)', 
    re.IGNORECASE
)

# Load invalid terms if possible
def load_invalid_terms():
    import json
    try:
        with open(INVALID_TERMS_FILE, 'r') as f:
            data = json.load(f)
            return set(t.lower() for t in data.get("invalid_speakers", []))
    except:
        return set()

def audit_summaries():
    summary_files = glob.glob(os.path.join(DATA_DIR, "*_Summary.csv"))
    invalid_terms = load_invalid_terms()
    
    results = [] # List of dicts for CSV output
    
    print(f"Scanning {len(summary_files)} summary files...")

    for file_path in summary_files:
        filename = os.path.basename(file_path)
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    speaker = row.get('speaker', '').strip()
                    title = row.get('title', '').strip()
                    desc = row.get('description', '').strip()
                    
                    # 1. Check for Missed (Unknown but looks like it has a name)
                    if not speaker or speaker == "Unknown Speaker":
                        match = POTENTIAL_NAME_REGEX.search(title)
                        if match:
                             # Exclude if the match is just a common phrase
                             name_candidate = match.group(1)
                             if name_candidate.lower() not in ["jesus christ", "lord jesus", "prophet lamb"]:
                                 # Additional check for Lamb of God patterns
                                 if "lamb of god" in name_candidate.lower() or "king of the jews" in name_candidate.lower():
                                     continue
                                     
                                 results.append({
                                    "filename": filename,
                                    "issue_type": "Potential Missed Speaker",
                                    "speaker_or_match": match.group(0),
                                    "title": title,
                                    "reason": "Pattern found in title but speaker is Unknown"
                                })
                        continue

                    # 2. Check for Suspicious Detected Speakers
                    reason = []
                    
                    # A. Matches Title (High chance of error if title is long)
                    if speaker.lower() == title.lower() and len(speaker) > 20: 
                        reason.append("Matches Long Title")
                    
                    # B. Contains digits
                    if any(c.isdigit() for c in speaker):
                        reason.append("Contains Digits")
                        
                    # C. Contains Dates (Months)
                    if re.search(r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\b', speaker, re.IGNORECASE):
                        reason.append("Contains Month")

                    # D. Too Long (> 4 words)
                    if len(speaker.split()) > 4:
                        reason.append("Too Long (>4 words)")
                        
                    # E. Too Short (< 3 chars)
                    if len(speaker) < 3:
                        reason.append("Too Short")
                        
                    # F. In Invalid List
                    if speaker.lower() in invalid_terms:
                        reason.append("In Invalid List")
                        
                    # G. Suspicious Words (Prepositions/Conjunctions at start/end or weird places)
                    if re.search(r'^(the|a|an|in|on|at|to|for|of|by|with)\b', speaker, re.IGNORECASE):
                         reason.append("Starts with Stop Word")
                    
                    if re.search(r'\b(and|or)\b', speaker, re.IGNORECASE):
                         # "Bro X and Bro Y" is valid. "Word and Spirit" is not.
                         # Simple heuristic: if "Bro" or "Pastor" not present, flag "and"
                         if "bro" not in speaker.lower() and "pastor" not in speaker.lower() and "sis" not in speaker.lower():
                             reason.append("Contains 'and' without Honorific")

                    if reason:
                        results.append({
                            "filename": filename,
                            "issue_type": "Suspicious Speaker",
                            "speaker_or_match": speaker,
                            "title": title,
                            "reason": "; ".join(reason)
                        })

        except Exception as e:
            pass

    # Sort results by issue_type then filename
    results.sort(key=lambda x: (x['issue_type'], x['filename']))

    # Console Summary
    suspicious_count = sum(1 for r in results if r['issue_type'] == "Suspicious Speaker")
    missed_count = sum(1 for r in results if r['issue_type'] == "Potential Missed Speaker")
    
    print("-" * 50)
    print(f"Suspicious Speakers: {suspicious_count}")
    print(f"Potential Missed Speakers: {missed_count}")
    
    # Ensure logs directory exists
    os.makedirs("logs/audit", exist_ok=True)
    output_file = "logs/audit/audit_results.csv"
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ["filename", "issue_type", "speaker_or_match", "title", "reason"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print("-" * 50)
        print(f"Full audit log written to {output_file}")
    except Exception as e:
        print(f"Error writing CSV: {e}")

if __name__ == "__main__":
    audit_summaries()
