#!/usr/bin/env python3
"""
Fix the Location and Duration fields in William Branham sermon transcripts.

The original scraping put state codes (like "LA", "CA") in the Duration field
instead of in the Location field. This script:
1. Extracts the state code from Duration
2. Appends it to Location  
3. Fixes Duration to only contain the time in minutes

Examples of broken data:
  Location: Shreveport
  Duration: LA, 37 min
  
Should become:
  Location: Shreveport, LA
  Duration: 37 min
"""

import os
import re

DATA_DIR = "data/William_Branham_Sermons"

# US State codes (2-letter abbreviations)
US_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'PR', 'VI'
}

def fix_file(filepath):
    """Fix location/duration fields in a single file."""
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()
    
    # Find the header section (before "--- TRANSCRIPT ---")
    if "--- TRANSCRIPT ---" not in content:
        return False, "No transcript marker found"
    
    header, transcript = content.split("--- TRANSCRIPT ---", 1)
    
    # Extract current Location and Duration
    location_match = re.search(r"Location:\s*(.+)", header)
    duration_match = re.search(r"Duration:\s*(.+)", header)
    
    if not location_match or not duration_match:
        return False, "Missing Location or Duration"
    
    location = location_match.group(1).strip()
    duration = duration_match.group(1).strip()
    
    # Check if Duration has the broken format (contains comma with state/city info)
    # Pattern: "LA, 37 min" or "Connersville, IN, 97 min"
    if ',' not in duration:
        return False, "Duration already clean (no comma)"
    
    # Parse the duration field
    # Could be: "LA, 37 min" or "Connersville, IN, 97 min"
    parts = [p.strip() for p in duration.split(',')]
    
    # Find the minutes part (should contain "min" or be a number)
    minutes_part = None
    location_parts = []
    
    for part in parts:
        if 'min' in part.lower() or re.match(r'^\d+$', part):
            minutes_part = part
        else:
            location_parts.append(part)
    
    if not minutes_part:
        # Try to find a number in the duration
        num_match = re.search(r'(\d+)\s*min', duration)
        if num_match:
            minutes_part = f"{num_match.group(1)} min"
            # Everything before the number goes to location
            before_num = re.sub(r'\d+\s*min.*', '', duration).strip().rstrip(',').strip()
            if before_num:
                location_parts = [p.strip() for p in before_num.split(',') if p.strip()]
        else:
            return False, f"Could not parse duration: {duration}"
    
    # Build new location
    if location_parts:
        # Check if location_parts look like they should be added to location
        # (city name or state code)
        new_location_suffix = ', '.join(location_parts)
        
        # If location already ends with the same info, don't duplicate
        if new_location_suffix not in location:
            new_location = f"{location}, {new_location_suffix}"
        else:
            new_location = location
    else:
        new_location = location
    
    # Clean up the new location - remove duplicates
    # Sometimes we might get "Phoenix, AZ, AZ" if processed twice
    loc_parts = [p.strip() for p in new_location.split(',')]
    seen = []
    for p in loc_parts:
        if p and p not in seen:
            seen.append(p)
    new_location = ', '.join(seen)
    
    # Ensure minutes_part is clean (just "X min" format)
    min_match = re.search(r'(\d+)', minutes_part)
    if min_match:
        new_duration = f"{min_match.group(1)} min"
    else:
        new_duration = minutes_part
    
    # Replace in header
    new_header = re.sub(r"Location:\s*.+", f"Location: {new_location}", header)
    new_header = re.sub(r"Duration:\s*.+", f"Duration: {new_duration}", new_header)
    
    # Reconstruct content
    new_content = new_header + "--- TRANSCRIPT ---" + transcript
    
    # Only write if changed
    if new_content != content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        return True, f"Fixed: Location='{new_location}', Duration='{new_duration}'"
    
    return False, "No changes needed"

def main():
    print("=" * 60)
    print("Fix WMB Location/Duration Fields")
    print("=" * 60)
    
    if not os.path.exists(DATA_DIR):
        print(f"❌ Data directory not found: {DATA_DIR}")
        return
    
    files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.txt')])
    print(f"📖 Processing {len(files)} sermon files...")
    
    fixed = 0
    skipped = 0
    errors = 0
    
    for filename in files:
        filepath = os.path.join(DATA_DIR, filename)
        try:
            success, message = fix_file(filepath)
            if success:
                fixed += 1
                print(f"   ✓ {filename}: {message}")
            else:
                skipped += 1
                # Uncomment to see skip reasons:
                # print(f"   - {filename}: {message}")
        except Exception as e:
            errors += 1
            print(f"   ⚠️ {filename}: Error - {e}")
    
    print(f"\n{'=' * 60}")
    print(f"Summary:")
    print(f"  Fixed:   {fixed} files")
    print(f"  Skipped: {skipped} files (already clean or no changes)")
    print(f"  Errors:  {errors} files")
    print(f"{'=' * 60}")
    print(f"\nNow run: python generate_wmb_site_data.py")

if __name__ == "__main__":
    main()
