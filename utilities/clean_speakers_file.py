import sys
sys.path.append("..")
import json
import update_sermons

def clean_speakers():
    with open("speakers.json", "r") as f:
        speakers = json.load(f)
    
    print(f"Original speaker count: {len(speakers)}")
    
    valid_speakers = []
    removed_count = 0
    
    for speaker in speakers:
        # Check if valid using the UPDATED logic in update_sermons
        if update_sermons.is_valid_person_name(speaker):
            valid_speakers.append(speaker)
        else:
            print(f"Removing invalid speaker: {speaker}")
            removed_count += 1
            
    print(f"Removed {removed_count} speakers.")
    print(f"New speaker count: {len(valid_speakers)}")
    
    # Sort for consistency
    valid_speakers.sort()
    
    # Backup
    with open("speakers.json.bak_auto", "w") as f:
        json.dump(speakers, f, indent=4)
        
    with open("speakers.json", "w") as f:
        json.dump(valid_speakers, f, indent=4)

if __name__ == "__main__":
    clean_speakers()
