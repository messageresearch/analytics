#!/usr/bin/env python3
"""
Heal script for Spoken Word Church date issues.

The Spoken Word Church YouTube channel (@SpokenWordBride) uploaded many archived
videos at once, causing YouTube's upload date to be used instead of the actual
sermon date. The actual dates are embedded in video titles using YY-MMDD format:
  - 08-1214 = December 14, 2008
  - 10-0704am = July 4, 2010 (morning service)
  - 09-0521pm = May 21, 2009 (evening service)

This script:
1. Parses the YY-MMDD format from titles
2. Updates the CSV with corrected dates
3. Renames transcript files with correct dates
4. Updates the Date: header inside transcript files
"""

import os
import re
import csv
from pathlib import Path
from datetime import datetime

# Configuration
CHURCH_NAME = "Spoken_Word_Church"
DATA_DIR = Path("data")
DOCS_DATA_DIR = Path("docs/data") / CHURCH_NAME
CSV_FILE = DATA_DIR / f"{CHURCH_NAME}_Summary.csv"

# Regex to extract YY-MMDD(am/pm) from the start of a title
# Matches: 10-0704, 08-1214, 09-0521pm, 10-0411am, etc.
DATE_CODE_PATTERN = re.compile(r'^(\d{2})-(\d{2})(\d{2})(am|pm)?', re.IGNORECASE)


def parse_spoken_word_date(title):
    """
    Extract date from Spoken Word Church title format: YY-MMDD(am/pm)
    
    Args:
        title: Video title like "10-0704 - Daniel 70 Weeks Pt.25 - Wade Dale"
    
    Returns:
        tuple: (date_str in YYYY-MM-DD format, am/pm suffix or None, cleaned_title)
               Returns (None, None, title) if no date code found
    """
    match = DATE_CODE_PATTERN.match(title.strip())
    if not match:
        return None, None, title
    
    year_2digit = match.group(1)
    month = match.group(2)
    day = match.group(3)
    service_time = match.group(4)  # am or pm, or None
    
    # Convert 2-digit year to 4-digit (assumes 2000s for 00-29, 1900s for 30-99)
    year_int = int(year_2digit)
    if year_int <= 29:
        year_4digit = 2000 + year_int
    else:
        year_4digit = 1900 + year_int
    
    # Validate month and day
    try:
        month_int = int(month)
        day_int = int(day)
        if not (1 <= month_int <= 12 and 1 <= day_int <= 31):
            return None, None, title
        
        # Validate the full date
        date_obj = datetime(year_4digit, month_int, day_int)
        date_str = date_obj.strftime("%Y-%m-%d")
        
        return date_str, service_time, title
    except (ValueError, OverflowError):
        return None, None, title


def update_csv_dates(dry_run=False):
    """Update dates in the CSV file based on title date codes."""
    if not CSV_FILE.exists():
        print(f"CSV file not found: {CSV_FILE}")
        return {}
    
    updated_entries = {}
    rows = []
    
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        
        for row in reader:
            title = row.get('title', '')
            old_date = row.get('date', '')
            url = row.get('url', '')
            
            new_date, service_time, _ = parse_spoken_word_date(title)
            
            if new_date and new_date != old_date:
                updated_entries[url] = {
                    'old_date': old_date,
                    'new_date': new_date,
                    'title': title,
                    'service_time': service_time
                }
                row['date'] = new_date
            
            rows.append(row)
    
    if not dry_run and updated_entries:
        with open(CSV_FILE, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Updated {len(updated_entries)} entries in {CSV_FILE}")
    
    return updated_entries


def update_transcript_file(filepath, new_date, dry_run=False):
    """
    Update the Date: header inside a transcript file.
    
    Returns: True if updated, False otherwise
    """
    if not filepath.exists():
        return False
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"  Error reading {filepath}: {e}")
        return False
    
    # Update the Date: line in the header
    date_pattern = re.compile(r'^(Date:\s+)(\d{4}-\d{2}-\d{2}|\w+)', re.MULTILINE)
    match = date_pattern.search(content)
    
    if match:
        old_date = match.group(2)
        if old_date != new_date:
            new_content = date_pattern.sub(f'\\g<1>{new_date}', content, count=1)
            
            if not dry_run:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
            return True
    
    return False


def rename_transcript_file(old_path, new_date, dry_run=False):
    """
    Rename a transcript file with the correct date prefix.
    
    Old: 2013-01-04 - 10-0704 - Title - Speaker.txt
    New: 2010-07-04 - 10-0704 - Title - Speaker.txt
    
    Returns: new_path if renamed, None otherwise
    """
    if not old_path.exists():
        return None
    
    filename = old_path.name
    
    # Pattern to match: YYYY-MM-DD - rest of filename
    rename_pattern = re.compile(r'^(\d{4}-\d{2}-\d{2})(\s*-\s*.+\.txt)$')
    match = rename_pattern.match(filename)
    
    if match:
        old_date_prefix = match.group(1)
        rest_of_name = match.group(2)
        
        if old_date_prefix != new_date:
            new_filename = f"{new_date}{rest_of_name}"
            new_path = old_path.parent / new_filename
            
            if not dry_run:
                # Check if target exists (avoid overwriting)
                if new_path.exists() and new_path != old_path:
                    print(f"  Warning: {new_path.name} already exists, skipping rename")
                    return None
                old_path.rename(new_path)
            
            return new_path
    
    return None


def find_transcript_file(old_date, title, speaker):
    """
    Find the transcript file in docs/data/Spoken_Word_Church/
    """
    if not DOCS_DATA_DIR.exists():
        return None
    
    # Build expected filename pattern
    # Format: {date} - {title} - {speaker}.txt
    safe_title = title.replace('/', '-').replace('\\', '-')
    safe_speaker = speaker.replace('/', '-').replace('\\', '-')
    
    expected_name = f"{old_date} - {safe_title} - {safe_speaker}.txt"
    expected_path = DOCS_DATA_DIR / expected_name
    
    if expected_path.exists():
        return expected_path
    
    # Try fuzzy matching if exact match fails
    for f in DOCS_DATA_DIR.glob(f"{old_date}*{title[:20]}*.txt"):
        return f
    
    return None


def heal_spoken_word_dates(dry_run=True):
    """
    Main healing function.
    
    Args:
        dry_run: If True, only report what would be changed without making changes
    """
    print("=" * 70)
    print("Spoken Word Church Date Healing Script")
    print("=" * 70)
    print(f"Mode: {'DRY RUN (no changes will be made)' if dry_run else 'LIVE (making changes)'}")
    print()
    
    # Step 1: Update CSV dates
    print("Step 1: Analyzing CSV for date corrections...")
    updated_entries = update_csv_dates(dry_run=dry_run)
    
    if not updated_entries:
        print("No date corrections needed in CSV.")
        return
    
    print(f"Found {len(updated_entries)} entries needing date correction")
    print()
    
    # Step 2: Process transcript files
    print("Step 2: Processing transcript files...")
    files_renamed = 0
    files_updated = 0
    files_not_found = 0
    
    # Read CSV to get speaker info for file matching
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        csv_data = {row['url']: row for row in reader}
    
    for url, entry in updated_entries.items():
        old_date = entry['old_date']
        new_date = entry['new_date']
        title = entry['title']
        
        csv_row = csv_data.get(url, {})
        speaker = csv_row.get('speaker', 'Unknown Speaker')
        
        # Find the transcript file
        transcript_path = find_transcript_file(old_date, title, speaker)
        
        if transcript_path:
            # Update the Date: header inside the file
            if update_transcript_file(transcript_path, new_date, dry_run=dry_run):
                files_updated += 1
                if not dry_run:
                    print(f"  Updated header: {transcript_path.name}")
            
            # Rename the file
            new_path = rename_transcript_file(transcript_path, new_date, dry_run=dry_run)
            if new_path:
                files_renamed += 1
                if dry_run:
                    print(f"  Would rename: {transcript_path.name}")
                    print(f"           to: {new_path.name}")
                else:
                    print(f"  Renamed: {new_path.name}")
        else:
            files_not_found += 1
            if dry_run:
                print(f"  File not found for: {old_date} - {title[:50]}...")
    
    # Summary
    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"CSV entries updated:     {len(updated_entries)}")
    print(f"Transcript files renamed: {files_renamed}")
    print(f"Transcript headers fixed: {files_updated}")
    print(f"Files not found:         {files_not_found}")
    
    if dry_run:
        print()
        print("This was a DRY RUN. To apply changes, run with --apply flag")


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Fix dates for Spoken Word Church transcripts"
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Actually apply the changes (default is dry run)'
    )
    parser.add_argument(
        '--test',
        type=str,
        help='Test date parsing on a single title'
    )
    
    args = parser.parse_args()
    
    if args.test:
        date, service, title = parse_spoken_word_date(args.test)
        print(f"Title:   {args.test}")
        print(f"Date:    {date}")
        print(f"Service: {service}")
        return
    
    heal_spoken_word_dates(dry_run=not args.apply)


if __name__ == "__main__":
    main()
