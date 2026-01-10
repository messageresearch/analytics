import sys
sys.path.append("..")
from update_sermons import smart_speaker_correction, is_valid_person_name

print("\n--- TESTING smart_speaker_correction ---")
names = [
    "Alex Perez, Fernando Avila",
    "Alex Perez, Jesus Salgado, Marcial Tapia, Tito Moreira",
    "Redemption",
    "Intellectual"
]

for n in names:
    corrected = smart_speaker_correction(n, "")
    print(f"'{n}' -> '{corrected}'")
