import sys
sys.path.append("..")
import update_sermons

title = "Bro Andrew Spencer July 20, 2024"
desc = ""
known = set()

print(f"Testing Title: '{title}'")
result = update_sermons.identify_speaker_dynamic(title, desc, known)
print(f"Result: {result}")

title2 = "Bro. Ron Spencer February 26, 2023"
print(f"Testing Title: '{title2}'")
result2 = update_sermons.identify_speaker_dynamic(title2, desc, known)
print(f"Result: {result2}")

val = update_sermons.final_validation("A Fresh")
print(f"Validation check for 'A Fresh': '{val}'")
