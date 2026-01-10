import json
import re

SPEAKERS_FILE = "speakers.json"
BACKUP_FILE = "speakers.json.bak"

def clean_speakers_json():
    # Load
    try:
        with open(SPEAKERS_FILE, 'r', encoding='utf-8') as f:
            speakers = json.load(f)
    except Exception as e:
        print(f"Error loading {SPEAKERS_FILE}: {e}")
        return

    # Backup
    with open(BACKUP_FILE, 'w', encoding='utf-8') as f:
        json.dump(speakers, f, indent=2)

    cleaned_speakers = []
    removed_count = 0
    
    unique_set = set()

    for s in speakers:
        s_clean = s.strip()
        if not s_clean: continue
        
        lower_s = s_clean.lower()
        reason = None
        
        # Validation Logic (Same as new update_sermons logic + extra strictness)
        if len(s_clean.split()) > 4:
            # Allow long multi-speakers "Bro X and Bro Y"
            if " and " not in lower_s:
                reason = "Too Long"
        
        if len(s_clean) < 3:
            reason = "Too Short"
            
        if any(c.isdigit() for c in s_clean):
            reason = "Digit"
            
        if re.search(r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\b', lower_s):
            reason = "Date"
            
        if lower_s.startswith(("the ", "a ", "an ", "in ", "on ", "at ", "to ", "for ", "by ", "with ")):
            if not lower_s.startswith(("a. ", "a ")): # "A. W. Tozer"? "A Name"?
                 # "A " is tricky if it's initial. "A. Name".
                 # If it is "a " followed by a word starting with lowercase? No, everything is probably title case or mixed.
                 # "A Fresh" -> Remove.
                 # "A. J. Smith" -> Keep.
                 if re.match(r'^a\s+[a-zA-Z]{3,}', lower_s): # "A " + Word > 3 chars
                     reason = "Starts with Stop Word 'A'"
                 elif lower_s.startswith(("the ", "in ", "on ", "at ", "to ", "for ", "by ", "with ")):
                     reason = "Starts with Stop Word"

        if " and " in lower_s or " & " in lower_s:
             # Must have honorific if it has 'and', OR be clear names
             # Simply reject if it looks like a title
             if not any(h in lower_s for h in ["bro", "sis", "pas", "rev", "bish", "eld"]):
                 if lower_s not in ["frank and deborah"]: # hardcode exception? nah.
                     # Check if it looks like "Name Name and Name Name"
                     parts = re.split(r'\s+(?:and|&)\s+', s_clean)
                     valid_parts = True
                     if len(parts) != 2:
                         valid_parts = False
                     else:
                         for p in parts:
                             if len(p.split()) < 2: 
                                 valid_parts = False # "Jim and" -> False. "Jim Smith" -> True
                     
                     if not valid_parts:
                        reason = "Weak Multi-Speaker"

        if reason:
            print(f"Removing '{s_clean}' ({reason})")
            removed_count += 1
        else:
            if lower_s not in unique_set:
                cleaned_speakers.append(s_clean)
                unique_set.add(lower_s)
    
    cleaned_speakers.sort()
    
    with open(SPEAKERS_FILE, 'w', encoding='utf-8') as f:
        json.dump(cleaned_speakers, f, indent=2)
        
    print(f"Removed {removed_count} invalid speakers.")
    print(f"Detailed list saved to {SPEAKERS_FILE}")

if __name__ == "__main__":
    clean_speakers_json()
