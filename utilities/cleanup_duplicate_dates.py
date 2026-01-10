#!/usr/bin/env python3
"""
Clean up duplicate transcript files with wrong date prefixes.

After the heal_spoken_word_dates.py script renamed files, some duplicates
were created when a correctly-dated file already existed. This script
removes the wrong-dated duplicates.
"""

import os
import re
from pathlib import Path
from collections import defaultdict

SPOKEN_WORD_DIR = Path(__file__).parent / "data" / "Spoken_Word_Church"

# Known wrong date prefixes
WRONG_DATE_PREFIXES = ['2013-01-04', '2024-02-04']


def find_duplicates(dry_run=True):
    """
    Find duplicate transcript files where a correct-dated version exists.
    
    Returns a list of files to delete.
    """
    files_to_delete = []
    
    # Group files by their "rest" part (everything after the date prefix)
    files_by_rest = defaultdict(list)
    
    for filepath in SPOKEN_WORD_DIR.glob("*.txt"):
        filename = filepath.name
        # Match: YYYY-MM-DD - rest
        match = re.match(r'^(\d{4}-\d{2}-\d{2})(\s*-\s*.+)$', filename)
        if match:
            date_prefix = match.group(1)
            rest = match.group(2)
            files_by_rest[rest].append((date_prefix, filepath))
    
    # Find duplicates
    for rest, file_list in files_by_rest.items():
        if len(file_list) > 1:
            # Separate into wrong-dated and correct-dated
            wrong_dated = []
            correct_dated = []
            
            for date_prefix, filepath in file_list:
                if date_prefix in WRONG_DATE_PREFIXES:
                    wrong_dated.append(filepath)
                else:
                    correct_dated.append(filepath)
            
            # If there's at least one correct-dated file, mark wrong-dated ones for deletion
            if correct_dated and wrong_dated:
                for wrong_file in wrong_dated:
                    files_to_delete.append({
                        'wrong_file': wrong_file,
                        'correct_file': correct_dated[0]  # Keep the first correct one
                    })
    
    return files_to_delete


def cleanup_duplicates(dry_run=True):
    """Remove duplicate wrong-dated files."""
    duplicates = find_duplicates(dry_run)
    
    if not duplicates:
        print("No duplicate files found!")
        return
    
    print(f"Found {len(duplicates)} duplicate files to clean up")
    print()
    
    if dry_run:
        print("=== DRY RUN - No files will be deleted ===")
        print()
    
    deleted_count = 0
    for item in duplicates:
        wrong_file = item['wrong_file']
        correct_file = item['correct_file']
        
        print(f"DELETE: {wrong_file.name}")
        print(f"   KEEP: {correct_file.name}")
        
        if not dry_run:
            try:
                wrong_file.unlink()
                deleted_count += 1
            except Exception as e:
                print(f"   ERROR: {e}")
    
    print()
    if dry_run:
        print(f"Would delete {len(duplicates)} files. Run with --apply to delete.")
    else:
        print(f"Deleted {deleted_count} duplicate files.")


def remove_orphan_wrong_dates(dry_run=True):
    """
    Remove files with wrong date prefixes that don't have duplicates
    BUT the title contains a YY-MMDD code that should be the real date.
    
    These are files where the heal script couldn't find the source file
    because the filename didn't match the CSV expectation.
    """
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from heal_spoken_word_dates import parse_spoken_word_date
    
    orphans_to_delete = []
    
    # Group files by their "rest" part
    files_by_rest = defaultdict(list)
    
    for filepath in SPOKEN_WORD_DIR.glob("*.txt"):
        filename = filepath.name
        match = re.match(r'^(\d{4}-\d{2}-\d{2})(\s*-\s*.+)$', filename)
        if match:
            date_prefix = match.group(1)
            rest = match.group(2)
            files_by_rest[rest].append((date_prefix, filepath))
    
    for rest, file_list in files_by_rest.items():
        # Only look at files that are alone (no duplicate)
        if len(file_list) == 1:
            date_prefix, filepath = file_list[0]
            if date_prefix in WRONG_DATE_PREFIXES:
                # Check if title contains YY-MMDD code
                title_part = rest.strip(' -')
                correct_date, _, _ = parse_spoken_word_date(title_part)
                if correct_date and correct_date != date_prefix:
                    orphans_to_delete.append({
                        'filepath': filepath,
                        'wrong_date': date_prefix,
                        'correct_date': correct_date
                    })
    
    if not orphans_to_delete:
        print("No orphan wrong-dated files found!")
        return
    
    print(f"Found {len(orphans_to_delete)} orphan files with wrong dates")
    print()
    
    if dry_run:
        print("=== DRY RUN - Showing what would be renamed ===")
        print()
    
    renamed_count = 0
    for item in orphans_to_delete:
        filepath = item['filepath']
        correct_date = item['correct_date']
        
        # Build new filename
        filename = filepath.name
        match = re.match(r'^(\d{4}-\d{2}-\d{2})(\s*-\s*.+)$', filename)
        if match:
            new_filename = f"{correct_date}{match.group(2)}"
            new_filepath = filepath.parent / new_filename
            
            print(f"RENAME: {filepath.name}")
            print(f"     -> {new_filename}")
            
            if not dry_run:
                try:
                    # Also update the Date header in the file
                    content = filepath.read_text(encoding='utf-8')
                    new_content = re.sub(
                        r'^Date:\s*\d{4}-\d{2}-\d{2}',
                        f'Date: {correct_date}',
                        content,
                        count=1,
                        flags=re.MULTILINE
                    )
                    
                    # Write to new file
                    new_filepath.write_text(new_content, encoding='utf-8')
                    
                    # Delete old file
                    filepath.unlink()
                    renamed_count += 1
                except Exception as e:
                    print(f"   ERROR: {e}")
    
    print()
    if dry_run:
        print(f"Would rename {len(orphans_to_delete)} files. Run with --apply to rename.")
    else:
        print(f"Renamed {renamed_count} files.")


if __name__ == "__main__":
    import sys
    
    dry_run = "--apply" not in sys.argv
    
    print("=" * 60)
    print("STEP 1: Removing duplicate files (where correct version exists)")
    print("=" * 60)
    cleanup_duplicates(dry_run)
    
    print()
    print("=" * 60)
    print("STEP 2: Renaming orphan files with wrong dates")
    print("=" * 60)
    remove_orphan_wrong_dates(dry_run)
