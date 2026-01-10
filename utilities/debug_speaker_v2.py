import sys
sys.path.append("..")
import update_sermons
import re

title = "Annual Meetings 2025 Bro. Darrell Ward 71925"
desc = ""
known = set()

print(f"Testing Title: '{title}'")
result = update_sermons.identify_speaker_dynamic(title, desc, known)
print(f"Result: {result}")

title2 = "Bro. Joel Pruitt No Longer Slaves Wednesday Night 8-28-24"
print(f"Testing Title: '{title2}'")
result2 = update_sermons.identify_speaker_dynamic(title2, desc, known)
print(f"Result: {result2}")

title3 = "End Time Expectations"
print(f"Testing Title: '{title3}'")
result3 = update_sermons.identify_speaker_dynamic(title3, desc, known)
print(f"Result: {result3}")

# Test Regex directly
p = update_sermons.NAME_PATTERN
print(f"Name Regex: {p}")
match = re.search(update_sermons.extract_speaker_pattern1.__doc__ or "Pattern1", title2) 
# extract_speaker_pattern1 uses HONORIFIC_PATTERN + NAME_PATTERN
