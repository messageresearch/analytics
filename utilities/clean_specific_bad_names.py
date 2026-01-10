import json
import os

def clean():
    with open("speakers.json", "r") as f:
        data = json.load(f)
    
    original_count = len(data)
    new_data = []
    
    # Block terms (partial match, case-insensitive)
    # Based on user request and my analysis of bad patterns
    block_terms = [
        "tabernaculo", 
        "obra maestra", 
        "spiritual", 
        "supernatural", 
        "throw jonah", 
        "jonas", 
        "overboard",
        "genetics"
    ]
    
    removed = []
    
    for name in data:
        keep = True
        name_lower = name.lower()
        
        # Exact match check
        if name == "I":
            keep = False
        # Partial match check
        else:
            for term in block_terms:
                if term in name_lower:
                    keep = False
                    break
        
        if keep:
            new_data.append(name)
        else:
            removed.append(name)
            
    print(f"Removed {len(removed)} speakers:")
    for r in sorted(removed):
        print(f" - {r}")
        
    with open("speakers.json", "w") as f:
        json.dump(sorted(new_data), f, indent=4)
        
    print(f"Saved speakers.json. Count {original_count} -> {len(new_data)}")

if __name__ == "__main__":
    clean()
