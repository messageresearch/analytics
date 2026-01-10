import json
import os
import re
from collections import defaultdict

SPEAKERS_FILE = "speakers.json"
CONFIG_FILE = "../config/speakers_config.json"

def clean_name(name):
    """Strip common noise chars."""
    return name.strip(" -.,")

def scan_clusters():
    if not os.path.exists(SPEAKERS_FILE):
        print("❌ speakers.json not found.")
        return

    with open(SPEAKERS_FILE, 'r', encoding='utf-8') as f:
        speakers = json.load(f)

    print(f"🔍 Scanning {len(speakers)} speakers for prefix patterns...")
    
    # Group by first 2 words (Title Case)
    clusters = defaultdict(list)
    
    for sp in speakers:
        parts = sp.split()
        if len(parts) >= 2:
            # Create a prefix key (First Last)
            prefix = f"{parts[0]} {parts[1]}"
            # Only consider it a cluster key if the prefix itself looks like a name
            if len(prefix) > 4: 
                clusters[prefix].append(sp)

    # Filter for clusters with multiple DISTINCT variants
    significant_clusters = {}
    for prefix, variants in clusters.items():
        # Clean naming variances
        unique_variants = sorted(list(set(variants)))
        
        # We are looking for cases where we have the Base Name + Longer versions
        # OR just multiple longer versions that share the base
        if len(unique_variants) > 1:
            # Check if one of them is the prefix itself?
            # Even if "Chad Lamb" isn't in the list, if we have "Chad Lamb Car" and "Chad Lamb Worthy",
            # the common denominator is "Chad Lamb".
            significant_clusters[prefix] = unique_variants

    print(f"✅ Found {len(significant_clusters)} clusters with potential issues.\n")
    
    # Sort by cluster size (most egregious first)
    sorted_clusters = sorted(significant_clusters.items(), key=lambda x: len(x[1]), reverse=True)
    
    new_rules = {}
    
    for prefix, variants in sorted_clusters:
        print(f"📂 CLUSTER: {prefix}")
        for v in variants:
            print(f"   - {v}")
            # Don't create a rule mapping "Chad Lamb" to "Chad Lamb"
            if v != prefix:
                new_rules[v] = prefix
        print("")

    if not new_rules:
        print("✨ No new normalization rules generated.")
        return

    # Offer to update config
    print("-" * 50)
    print(f"💡 Generated {len(new_rules)} suggested normalization rules.")
    
    # Load existing config to check for dupes
    current_rules = {}
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                data = json.load(f)
                current_rules = data.get("normalization_rules", {})
        except: pass
    
    # Filter out rules that already exist
    final_rules = {}
    for k, v in new_rules.items():
        if k not in current_rules:
            final_rules[k] = v
            
    if not final_rules:
        print("👍 All suggested rules are already in your config!")
        return

    print("snippet for config/speakers_config.json:")
    print(json.dumps(final_rules, indent=2))
    
    # Optional: write to a file for easy copying
    with open("suggested_rules.json", "w") as f:
        json.dump(final_rules, f, indent=2)
    print("\n💾 Saved suggestions to 'suggested_rules.json'")

if __name__ == "__main__":
    scan_clusters()
