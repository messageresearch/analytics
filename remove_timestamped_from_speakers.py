import json

with open('speakers.json', 'r', encoding='utf-8') as f:
    speakers = json.load(f)

filtered = [s for s in speakers if not s.endswith('.timestamped')]
removed_count = len(speakers) - len(filtered)

with open('speakers.json', 'w', encoding='utf-8') as f:
    json.dump(filtered, f, ensure_ascii=False, indent=2)

print(f"Removed {removed_count} names ending with .timestamped from speakers.json")
