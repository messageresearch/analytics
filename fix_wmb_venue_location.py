#!/usr/bin/env python3
"""
Fix venue and location fields in William Branham sermon transcripts.

Problems to fix:
1. Venue contains title fragments (e.g., "Today", "We Would See Jesus", "Lord")
2. Location contains "Venue, City, STATE" instead of just "City, STATE"

The pattern is: when venue has a title fragment, the actual venue is in the 
first part of location, and the city/state is in the rest of location.

Example before:
  Venue: Today
  Location: And Forever, Ramada Inn, Tucson, AZ

Example after:
  Venue: Ramada Inn
  Location: Tucson, AZ
"""

import os
import re

DATA_DIR = "data/William_Branham_Sermons"

# Known title fragments that ended up in Venue field
TITLE_FRAGMENTS = [
    'Today', 'And Forever', 'Lord', 'Do It', 'Sir?', 'Sirs',
    'We Would See Jesus', 'What Of The Night?', 'Elijah?', 'Dying',
    'Then Where Is All The Miracles?', 'Then Where Is All The Miracles',
    'Chapter Four #1', 'Chapter Four #2', 'Chapter Four #3',
    'Chapter Five #1', 'Chapter Five #2',
    'It Is I', 'Awake Jesus', 'They Knew Him',
    'Locust', 'Cankerworm', 'Caterpillar',
    'A Greater Than All Of Them Is Here',
    'A Greater Than All Of Them Is',
    'Then Revealing Himself In The',
    'Religious Realm', 'Physical Realm',
    'I Believe', 'Show Us The Father And It Sufficeth Us',
    'Come And See',
]

# Known venue keywords to identify actual venues
VENUE_KEYWORDS = [
    'Tabernacle', 'Church', 'Temple', 'Hall', 'Auditorium', 'Arena',
    'School', 'Center', 'Coliseum', 'Hotel', 'Inn', 'Armory',
    'Stadium', 'Assembly', 'Civic', 'Fairground', 'Amphitheater',
    'Legion', 'Gospel', 'Chapel'
]

# US State codes
US_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'PR', 'VI', 'BC', 'AB', 'SK', 'MB', 'ON', 'QC'  # Canadian provinces too
}

def is_title_fragment(venue):
    """Check if venue looks like a title fragment."""
    if not venue:
        return False
    # Direct match
    for frag in TITLE_FRAGMENTS:
        if venue == frag or venue.startswith(frag):
            return True
    # Has question mark (likely title)
    if '?' in venue:
        return True
    # Very short and no venue keywords
    if len(venue) < 15 and not any(kw in venue for kw in VENUE_KEYWORDS):
        # Check if it looks like a title word
        if venue[0].isupper() and ' ' not in venue:
            return True
    return False

def has_venue_keyword(text):
    """Check if text contains venue keywords."""
    return any(kw in text for kw in VENUE_KEYWORDS)

def extract_venue_and_location(location_str):
    """
    Extract venue and city/state from a combined location string.
    
    Input: "Ramada Inn, Tucson, AZ" or "And Forever, Ramada Inn, Tucson, AZ"
    Output: ("Ramada Inn", "Tucson, AZ")
    """
    if not location_str:
        return None, None
    
    parts = [p.strip() for p in location_str.split(',')]
    
    # Find where the city/state starts
    # State code is usually at the end
    state_idx = -1
    for i, part in enumerate(parts):
        if part.upper() in US_STATES:
            state_idx = i
            break
    
    if state_idx == -1:
        # No state found, can't parse reliably
        return None, None
    
    # City is usually right before state
    city_idx = state_idx - 1 if state_idx > 0 else state_idx
    
    # Everything before city is potential venue
    venue_parts = parts[:city_idx]
    location_parts = parts[city_idx:]
    
    # Filter out title fragments from venue parts
    filtered_venue_parts = []
    for vp in venue_parts:
        is_frag = False
        for frag in TITLE_FRAGMENTS:
            if vp == frag or vp.startswith(frag):
                is_frag = True
                break
        if not is_frag:
            filtered_venue_parts.append(vp)
    
    venue = ', '.join(filtered_venue_parts) if filtered_venue_parts else None
    location = ', '.join(location_parts)
    
    return venue, location

def fix_file(filepath):
    """Fix venue/location fields in a single file."""
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()
    
    if "--- TRANSCRIPT ---" not in content:
        return False, "No transcript marker"
    
    header, transcript = content.split("--- TRANSCRIPT ---", 1)
    
    venue_match = re.search(r"Venue:\s*(.+)", header)
    location_match = re.search(r"Location:\s*(.+)", header)
    
    if not venue_match or not location_match:
        return False, "Missing Venue or Location"
    
    venue = venue_match.group(1).strip()
    location = location_match.group(1).strip()
    
    original_venue = venue
    original_location = location
    
    # Check if venue is a title fragment
    if is_title_fragment(venue):
        # Try to extract real venue from location
        extracted_venue, extracted_location = extract_venue_and_location(location)
        
        if extracted_venue and has_venue_keyword(extracted_venue):
            venue = extracted_venue
            location = extracted_location
    
    # Also check if location has venue mixed in even if venue looks OK
    # Pattern: location starts with a venue name, then city, state
    elif has_venue_keyword(location.split(',')[0]) if ',' in location else False:
        parts = [p.strip() for p in location.split(',')]
        # Find state code
        for i, part in enumerate(parts):
            if part.upper() in US_STATES and i > 1:
                # Location has format: Venue, City, State
                # Move venue part to venue field if current venue is generic
                potential_venue = parts[0]
                potential_location = ', '.join(parts[1:])
                
                # Only change if it improves things
                if has_venue_keyword(potential_venue):
                    # Check if current venue is less specific
                    if not has_venue_keyword(venue) or venue == 'Unknown Venue':
                        venue = potential_venue
                        location = potential_location
                break
    
    # No changes needed
    if venue == original_venue and location == original_location:
        return False, "No changes needed"
    
    # Apply changes
    new_header = re.sub(r"Venue:\s*.+", f"Venue: {venue}", header)
    new_header = re.sub(r"Location:\s*.+", f"Location: {location}", new_header)
    
    new_content = new_header + "--- TRANSCRIPT ---" + transcript
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    return True, f"Venue: '{original_venue}' → '{venue}', Location: '{original_location}' → '{location}'"

def main():
    print("=" * 70)
    print("Fix WMB Venue/Location Fields")
    print("=" * 70)
    
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
                print(f"   ✓ {filename}")
                print(f"      {message}")
            else:
                skipped += 1
        except Exception as e:
            errors += 1
            print(f"   ⚠️ {filename}: {e}")
    
    print(f"\n{'=' * 70}")
    print(f"Summary:")
    print(f"  Fixed:   {fixed} files")
    print(f"  Skipped: {skipped} files")
    print(f"  Errors:  {errors} files")
    print(f"{'=' * 70}")
    print(f"\nNext steps:")
    print(f"  1. python3 generate_wmb_site_data.py")
    print(f"  2. cp -r data/William_Branham_Sermons/* public/data/William_Branham_Sermons/")

if __name__ == "__main__":
    main()
