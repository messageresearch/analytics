#!/usr/bin/env python3
"""
Split WMB_Sermons_Combined_Text.txt into individual sermon files.
Each sermon becomes a text file with metadata headers compatible with the site data generator.
"""

import os
import re
import sys
from pathlib import Path

# Configuration
INPUT_FILE = "/Users/johncalvo/Documents/WMB/WMBMessages/WMB_Sermons_Combined_Text.txt"
OUTPUT_DIR = "../data/William_Branham_Sermons"

# Regex to match sermon headers like:
# [47-0412, Faith Is The Substance, Oakland Municipal Auditorium, Oakland, CA, 79 min]
SERMON_HEADER_PATTERN = re.compile(
    r'\[(\d{2})-(\d{4}[A-Za-z]?),\s*'  # Date code: YY-MMDD with optional letter suffix
    r'([^,]+),\s*'                      # Title
    r'([^,]+),\s*'                      # Location/Venue
    r'([^,]+),\s*'                      # City, State (combined)
    r'([^\]]*)\]'                       # Duration or "no audio"
)

def parse_date_code(year_code, date_code):
    """Convert YY-MMDD to YYYY-MM-DD format."""
    # Year is 19XX for all of these sermons (1947-1965)
    year = f"19{year_code}"
    
    # Handle date_code which can be MMDD or MMDD + letter suffix
    date_part = re.match(r'(\d{4})', date_code)
    if date_part:
        mmdd = date_part.group(1)
        month = mmdd[:2]
        day = mmdd[2:]
        
        # Handle edge cases like 0000 (unknown date)
        if month == "00":
            month = "01"
        if day == "00":
            day = "01"
        
        return f"{year}-{month}-{day}"
    
    return f"{year}-01-01"  # Fallback

def sanitize_filename(title, date_code):
    """Create a safe filename from title and date."""
    # Remove or replace problematic characters
    safe_title = re.sub(r'[<>:"/\\|?*]', '', title)
    safe_title = re.sub(r'\s+', ' ', safe_title).strip()
    safe_title = safe_title[:100]  # Limit length
    return f"{date_code} - {safe_title}.txt"

def extract_duration_minutes(duration_str):
    """Extract duration in minutes from string like '79 min' or 'no audio'."""
    if not duration_str or 'no audio' in duration_str.lower():
        return 0
    match = re.search(r'(\d+)', duration_str)
    return int(match.group(1)) if match else 0

def split_sermons(input_file, output_dir):
    """Split the combined file into individual sermon files."""
    
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        sys.exit(1)
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    print(f"📖 Reading {input_file}...")
    with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()
    
    # Split on sermon headers, keeping the headers
    # We'll find all header positions first
    headers = list(SERMON_HEADER_PATTERN.finditer(content))
    
    print(f"📊 Found {len(headers)} sermon headers")
    
    sermons_created = 0
    errors = []
    
    for i, match in enumerate(headers):
        try:
            year_code = match.group(1)
            date_code = match.group(2)
            title = match.group(3).strip()
            venue = match.group(4).strip()
            location = match.group(5).strip()
            duration_str = match.group(6).strip()
            
            # Parse date
            full_date = parse_date_code(year_code, date_code)
            
            # Get sermon text (from end of header to start of next header or end of file)
            start_pos = match.end()
            end_pos = headers[i + 1].start() if i + 1 < len(headers) else len(content)
            sermon_text = content[start_pos:end_pos].strip()
            
            # Clean up the sermon text
            # Remove page markers like "Page 1 of 21"
            sermon_text = re.sub(r'Page \d+ of \d+\s*', '', sermon_text)
            # Remove "Courtesy of Living Word Broadcast" and "NOT FOR SALE"
            sermon_text = re.sub(r'Courtesy of Living Word Broadcast\s*', '', sermon_text)
            sermon_text = re.sub(r'NOT FOR SALE\s*', '', sermon_text)
            # Remove repeated header lines within the text
            sermon_text = re.sub(r'\d{2}-\d{4}[A-Za-z]?,\s*[^,]+,\s*[^,]+,\s*[^,]+,\s*\d+\s*min\s*', '', sermon_text)
            
            # Estimate word count
            word_count = len(sermon_text.split())
            
            # Create filename
            original_date_code = f"{year_code}-{date_code}"
            filename = sanitize_filename(title, original_date_code)
            filepath = os.path.join(output_dir, filename)
            
            # Build metadata header
            metadata_header = f"""Date: {full_date}
Title: {title}
Speaker: William Branham
Type: Sermon
Language: English
Venue: {venue}
Location: {location}
Duration: {duration_str}
Source: table.branham.org
DateCode: {original_date_code}

--- TRANSCRIPT ---

"""
            
            # Write the file
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(metadata_header + sermon_text)
            
            sermons_created += 1
            
            if sermons_created % 100 == 0:
                print(f"  ✓ Created {sermons_created} sermons...")
        
        except Exception as e:
            errors.append((match.group(0), str(e)))
    
    print(f"\n✅ Successfully created {sermons_created} sermon files")
    print(f"📁 Output directory: {output_dir}")
    
    if errors:
        print(f"\n⚠️  {len(errors)} errors occurred:")
        for header, error in errors[:10]:
            print(f"  - {header[:50]}...: {error}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more errors")
    
    return sermons_created

def main():
    print("=" * 60)
    print("William Branham Sermon Splitter")
    print("=" * 60)
    
    count = split_sermons(INPUT_FILE, OUTPUT_DIR)
    
    print("\n" + "=" * 60)
    print(f"Done! {count} sermons ready for processing.")
    print("Next step: Run generate_wmb_site_data.py")
    print("=" * 60)

if __name__ == "__main__":
    main()
