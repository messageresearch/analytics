import sys
sys.path.append("..")
import update_sermons
import re

# Mock known speakers
known_speakers = set()

titles = [
    ("24-0114M This is Uniting Time  Bro David Mayeur  Sunday Morning Service", "Bro David Mayeur"),
    ("Who Is Like Unto My God? - Bro. David Mayeur on October 2, 2022 at Evening Light Tabernacle", "Bro. David Mayeur"),
    ("24-0614E Return to Your First Love  Bro martin Shalom  Overnight Service", "Bro martin Shalom"),
    ("To Know As I Am Known - Bro. David Mayeur", "Bro. David Mayeur")
]

print("Debugging Speaker Detection...\n")

for title, expected in titles:
    print(f"Title: {title}")
    
    # 1. Test NAME_PATTERN directly
    print(f"  NAME_PATTERN raw check:")
    match = re.search(update_sermons.HONORIFIC_PATTERN + r'[\s\.]+' + update_sermons.NAME_PATTERN, title, re.IGNORECASE)
    if match:
        print(f"    Match found (IGNORECASE): {match.group(0)}")
    else:
        print(f"    No match (IGNORECASE)")
        
    match_strict = re.search(update_sermons.HONORIFIC_PATTERN + r'[\s\.]+' + update_sermons.NAME_PATTERN, title)
    if match_strict:
        print(f"    Match found (STRICT): {match_strict.group(0)}")
    else:
        print(f"    No match (STRICT)")

    # 2. Test identify_speaker_dynamic
    result = update_sermons.identify_speaker_dynamic(title, "", known_speakers)
    print(f"  identify_speaker_dynamic result: {result}")
    
    if result:
        detected = result[0] if isinstance(result, tuple) else result
        print(f"  Detected: '{detected}'")
        
        # Test validation
        valid = update_sermons.is_valid_person_name(detected)
        print(f"  is_valid_person_name('{detected}'): {valid}")
        
    print("-" * 30)

