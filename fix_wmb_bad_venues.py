#!/usr/bin/env python3
"""
Fix WMB sermon venues that contain sermon title fragments instead of actual venues.
"""

import os
from pathlib import Path

# Map dateCode -> correct venue based on location research
VENUE_FIXES = {
    '63-0628M': 'Associated Brotherhood Of Christians Campground',  # Hot Springs, AR
    '63-0627': 'Associated Brotherhood Of Christians Campground',   # Hot Springs, AR
    '63-0601': 'House Meeting',  # Tucson, AZ
    '61-0414': 'Illinois Wesleyan University',  # IL
    '60-0607': 'Miami Valley Chautauqua Campgrounds',  # Chautauqua, OH
    '60-0305': 'Madison Square Garden',  # Phoenix, AZ
    '60-0221': 'Branham Tabernacle',  # Jeffersonville, IN
    '59-0823': 'Branham Tabernacle',  # Jeffersonville, IN
    '59-0811': 'Miami Valley Chautauqua Campgrounds',  # Chautauqua, OH
    '59-0403': 'Angelus Temple',  # Los Angeles, CA
    '59-0329S': 'Branham Tabernacle',  # Jeffersonville, IN
    '58-1012': 'Branham Tabernacle',  # Jeffersonville, IN
    '58-0223': 'Memorial Auditorium',  # Chattanooga, TN
    '58-0202': 'The Hippodrome',  # Waterloo, IA
    '55-1119': 'Tent Meeting',  # San Fernando, CA
    '55-1114': 'Tent Meeting',  # San Fernando, CA
    '55-1113': 'Tent Meeting',  # San Fernando, CA
    '55-0806': 'Methodist Campgrounds',  # Campbellsville, KY
    '52-1027': 'Unknown',  # Edmonton, AB
    '48-0305': 'Unknown',  # Phoenix, AZ (old recording)
}

def main():
    base_dir = Path('data/William_Branham_Sermons')
    fixed = 0

    for txt_file in base_dir.glob('*.txt'):
        if txt_file.name.endswith('.timestamped.txt'):
            continue
        
        # Extract dateCode from filename
        date_code = txt_file.name.split(' - ')[0] if ' - ' in txt_file.name else None
        
        if date_code in VENUE_FIXES:
            with open(txt_file, 'r') as f:
                content = f.read()
            
            lines = content.split('\n')
            for i, line in enumerate(lines[:15]):
                if line.startswith('Venue: '):
                    old_venue = line[7:]
                    new_venue = VENUE_FIXES[date_code]
                    lines[i] = f'Venue: {new_venue}'
                    print(f"Fixed {date_code}: '{old_venue}' -> '{new_venue}'")
                    fixed += 1
                    break
            
            with open(txt_file, 'w') as f:
                f.write('\n'.join(lines))

    print(f"\nTotal fixed: {fixed}")

if __name__ == '__main__':
    main()
