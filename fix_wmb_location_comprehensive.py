#!/usr/bin/env python3
"""
Comprehensive fix for WMB sermon location fields.

Issues fixed:
1. State-only locations (e.g., "AZ" -> extract city from venue or mark Unknown)
2. Venue at start of location (e.g., "Tent Meeting, San Fernando, CA" -> move venue)
3. Title fragments at start (e.g., "And Forever, Methodist..." -> remove fragment)
4. Unknown prefix (e.g., "Unknown, Phoenix, AZ" -> "Phoenix, AZ")
5. Missing comma before state (e.g., "Phoenix AZ" -> "Phoenix, AZ")
6. Bad data (e.g., "no audio]" -> "Unknown")
"""

import os
import re
from pathlib import Path

# US and Canadian state/province codes
STATES = {'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
          'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
          'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC','PR','BC','AB','SK','MB','ON','QC'}

# Common title fragments that end up in location
TITLE_FRAGMENTS = [
    'And Forever',
    'And Acting',
    'Cankerworm',
    'Caterpillar',
]

# Venue keywords that indicate location starts with a venue
VENUE_KEYWORDS = [
    'Tent Meeting',
    'House Meeting',
    'Camp Meeting',
    'Marc Ballroom',
    'Associated Brotherhood',
    'Miami Valley Chautauqua',
    'Tift County Courthouse',
    'Essex Theater',
    'The Hippodrome',
    'Branham Tabernacle',
    'Philadelphia Church',
    'Methodist Campgrounds',
    'Unknown,',
]

def fix_location(location, venue, title):
    """Fix a location string and return (new_location, new_venue, changes_made)."""
    original_location = location
    original_venue = venue
    changes = []
    
    # 1. State-only locations - check if venue contains the city
    if location.strip().upper() in STATES and len(location.strip()) <= 3:
        state = location.strip().upper()
        # Check if venue is just a city name (no spaces suggesting it's a venue name)
        # Common pattern: Venue: "Tacoma" Location: "WA" -> Location: "Tacoma, WA"
        if venue:
            # If venue looks like just a city (single word or two words), move it to location
            venue_words = venue.split()
            is_city_name = (
                len(venue_words) <= 2 and  # City names are typically 1-2 words
                not any(kw in venue.lower() for kw in ['church', 'tabernacle', 'temple', 'meeting', 'hall', 'theater', 'auditorium', 'campground'])
            )
            if is_city_name:
                location = f"{venue}, {state}"
                venue = 'Unknown'  # Clear the venue since we moved city to location
                changes.append(f"Moved city from Venue to Location: {original_venue} -> {location}")
            else:
                # Venue is an actual venue, can we extract city from it?
                # Pattern: "Branham Tabernacle Jeffersonville" -> city is Jeffersonville
                words = venue.split()
                if words:
                    last_word = words[-1]
                    # Check if last word could be a city
                    if last_word[0].isupper() and len(last_word) > 2:
                        location = f"{last_word}, {state}"
                        venue = ' '.join(words[:-1]) if len(words) > 1 else 'Unknown'
                        changes.append(f"Extracted city from Venue: {original_venue} -> Location: {location}, Venue: {venue}")
                    else:
                        location = f"Unknown, {state}"
                        changes.append(f"State-only, couldn't extract city: {state}")
        else:
            location = f"Unknown, {state}"
            changes.append(f"State-only with no venue: {state}")
    
    # 2. Remove title fragments from start
    for fragment in TITLE_FRAGMENTS:
        if location.startswith(fragment):
            # Find the next comma and remove everything before it
            parts = location.split(',', 1)
            if len(parts) > 1:
                location = parts[1].strip()
                changes.append(f"Removed title fragment '{fragment}'")
    
    # 3. Move venue keywords from location start to venue field
    for kw in VENUE_KEYWORDS:
        if location.startswith(kw):
            # Special case: "Unknown," at start - just remove it
            if kw == 'Unknown,':
                location = location[len(kw):].strip()
                changes.append("Removed 'Unknown,' prefix")
                continue
            
            # Split by comma and move venue part
            parts = location.split(',')
            if len(parts) >= 2:
                venue_part = parts[0].strip()
                location = ', '.join(p.strip() for p in parts[1:])
                if not venue or venue == 'Unknown':
                    venue = venue_part
                    changes.append(f"Moved venue '{venue_part}' from location")
                else:
                    # Venue already has content, prepend this
                    changes.append(f"Venue already set, removing '{venue_part}' from location")
    
    # 4. Fix missing comma before state (e.g., "Phoenix AZ" -> "Phoenix, AZ")
    match = re.search(r'([a-zA-Z]+) ([A-Z]{2})$', location)
    if match:
        city, state = match.groups()
        if state in STATES:
            location = re.sub(r'([a-zA-Z]+) ([A-Z]{2})$', r'\1, \2', location)
            changes.append(f"Added comma before state: {match.group(0)}")
    
    # 5. Fix bad data
    if 'audio' in location.lower() or location == 'no audio]':
        location = 'Unknown'
        changes.append("Fixed bad data")
    
    # 6. Clean up any double commas or trailing commas
    location = re.sub(r',\s*,', ',', location)
    location = location.strip(' ,')
    
    return location, venue, changes

def process_file(filepath):
    """Process a single WMB transcript file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        with open(filepath, 'r', encoding='latin-1') as f:
            content = f.read()
    
    lines = content.split('\n')
    modified = False
    changes = []
    
    location = None
    venue = None
    title = None
    location_idx = None
    venue_idx = None
    
    # Find Location, Venue, and Title fields
    for i, line in enumerate(lines[:20]):  # Only check first 20 lines
        if line.startswith('Title:'):
            title = line[6:].strip()
        elif line.startswith('Location:'):
            location = line[9:].strip()
            location_idx = i
        elif line.startswith('Venue:'):
            venue = line[6:].strip()
            venue_idx = i
    
    if location is None:
        return False, []
    
    # Apply fixes
    new_location, new_venue, fix_changes = fix_location(location, venue or '', title or '')
    
    if fix_changes:
        if new_location != location:
            lines[location_idx] = f'Location: {new_location}'
            modified = True
            changes.extend(fix_changes)
        
        if venue_idx is not None and new_venue and new_venue != venue:
            lines[venue_idx] = f'Venue: {new_venue}'
            modified = True
        elif venue_idx is None and new_venue and new_venue != 'Unknown':
            # Need to insert Venue line after Location
            lines.insert(location_idx + 1, f'Venue: {new_venue}')
            modified = True
    
    if modified:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
    
    return modified, changes

def main():
    base_dir = Path('/Users/johncalvo/github/wmbmentions.github.io/data/William_Branham_Sermons')
    
    if not base_dir.exists():
        print(f"Directory not found: {base_dir}")
        return
    
    total_fixed = 0
    all_changes = []
    
    for txt_file in sorted(base_dir.glob('*.txt')):
        if txt_file.name.endswith('.timestamped.txt'):
            continue
        
        modified, changes = process_file(txt_file)
        if modified:
            total_fixed += 1
            all_changes.append((txt_file.name, changes))
            print(f"Fixed: {txt_file.name}")
            for c in changes:
                print(f"       {c}")
    
    print(f"\n{'='*60}")
    print(f"Total files fixed: {total_fixed}")
    
    if total_fixed > 0:
        print("\nRemember to sync files:")
        print("  cp -r data/William_Branham_Sermons/* docs/data/William_Branham_Sermons/")
        print("  cp -r data/William_Branham_Sermons/* public/data/William_Branham_Sermons/")
        print("  python3 generate_wmb_site_data.py")

if __name__ == '__main__':
    main()
