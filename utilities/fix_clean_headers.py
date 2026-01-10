#!/usr/bin/env python3
"""
Fix Clean.txt Headers Script
=============================
This script fixes the mismatch between filenames and their internal START OF FILE headers.
Many files have " - Clean.txt" appended to the header but not to the actual filename.

Usage:
    python fix_clean_headers.py          # Dry run - show what would be fixed
    python fix_clean_headers.py --fix    # Actually fix the files
"""

import os
import re
import sys
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

def fix_clean_headers(dry_run=True):
    """
    Scan all .txt files in data folder and fix START OF FILE headers
    that don't match the actual filename.
    """
    fixed_count = 0
    skipped_count = 0
    error_count = 0
    
    print(f"{'DRY RUN - ' if dry_run else ''}Scanning for mismatched headers...")
    print(f"Data directory: {DATA_DIR}")
    print("=" * 60)
    
    for church_folder in os.listdir(DATA_DIR):
        church_path = os.path.join(DATA_DIR, church_folder)
        if not os.path.isdir(church_path):
            continue
        
        # Skip non-church folders (like backup folders, CSV files)
        if church_folder.startswith('.') or church_folder.endswith('.csv'):
            continue
            
        for filename in os.listdir(church_path):
            if not filename.endswith('.txt'):
                continue
                
            filepath = os.path.join(church_path, filename)
            
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Find the START OF FILE line
                match = re.search(r'START OF FILE: (.+)', content)
                if not match:
                    skipped_count += 1
                    continue
                
                header_filename = match.group(1).strip()
                actual_filename = filename
                
                # Check if they match (after removing the .txt for comparison)
                header_base = header_filename.replace('.txt', '').strip()
                actual_base = actual_filename.replace('.txt', '').strip()
                
                # Check for " - Clean" suffix mismatch
                needs_fix = False
                if header_base.endswith(' - Clean') and not actual_base.endswith(' - Clean'):
                    needs_fix = True
                elif header_filename != actual_filename:
                    # Other mismatches
                    needs_fix = True
                
                if needs_fix:
                    # Build the correct header line
                    new_content = re.sub(
                        r'START OF FILE: .+',
                        f'START OF FILE: {actual_filename}',
                        content,
                        count=1
                    )
                    
                    if dry_run:
                        print(f"Would fix: {church_folder}/{filename}")
                        print(f"  Header says: {header_filename}")
                        print(f"  Should be:   {actual_filename}")
                        print()
                    else:
                        with open(filepath, 'w', encoding='utf-8') as f:
                            f.write(new_content)
                        print(f"Fixed: {church_folder}/{filename}")
                    
                    fixed_count += 1
                    
            except Exception as e:
                error_count += 1
                print(f"Error processing {filepath}: {e}")
    
    print("=" * 60)
    print(f"Summary:")
    print(f"  Files {'would be ' if dry_run else ''}fixed: {fixed_count}")
    print(f"  Files skipped (no header): {skipped_count}")
    print(f"  Errors: {error_count}")
    
    if dry_run and fixed_count > 0:
        print(f"\nRun with --fix flag to actually fix these files:")
        print(f"  python fix_clean_headers.py --fix")
    
    return fixed_count, skipped_count, error_count


def main():
    dry_run = '--fix' not in sys.argv
    
    if not dry_run:
        print("=" * 60)
        print("FIXING FILES - This will modify files!")
        print("=" * 60)
        response = input("Are you sure you want to proceed? (y/N): ")
        if response.lower() != 'y':
            print("Aborted.")
            return
    
    start_time = datetime.now()
    fix_clean_headers(dry_run=dry_run)
    elapsed = datetime.now() - start_time
    print(f"\nCompleted in {elapsed.total_seconds():.2f} seconds")


if __name__ == '__main__':
    main()
