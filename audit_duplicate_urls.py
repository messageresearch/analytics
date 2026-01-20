#!/usr/bin/env python3
"""
Audit script to find duplicate URLs across all *_Summary.csv files in data/ folder.
"""

import os
import csv
from collections import defaultdict

DATA_DIR = "data"

def audit_duplicate_urls():
    """Scan all CSV files and identify duplicate URLs."""
    
    # Map URL -> list of (church, row_data)
    url_occurrences = defaultdict(list)
    
    # Statistics
    total_files = 0
    total_rows = 0
    urls_found = 0
    
    print("🔍 Scanning all *_Summary.csv files for duplicate URLs...\n")
    
    # Get all CSV files
    csv_files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('_Summary.csv')])
    
    for csv_file in csv_files:
        csv_path = os.path.join(DATA_DIR, csv_file)
        church_name = csv_file.replace('_Summary.csv', '')
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row_num, row in enumerate(reader, start=2):  # start=2 because row 1 is header
                    total_rows += 1
                    url = row.get('url', '').strip()
                    
                    if url and ('youtube.com' in url or 'youtu.be' in url):
                        urls_found += 1
                        # Store the occurrence with minimal data for comparison
                        occurrence = {
                            'church': church_name,
                            'file': csv_file,
                            'row': row_num,
                            'date': row.get('date', '').strip(),
                            'title': row.get('title', '').strip(),
                            'speaker': row.get('speaker', '').strip(),
                            'status': row.get('status', '').strip(),
                            'first_scraped': row.get('first_scraped', '').strip(),
                        }
                        url_occurrences[url].append(occurrence)
            
            total_files += 1
        except Exception as e:
            print(f"⚠️  Error reading {csv_file}: {e}")
    
    # Find duplicates
    duplicates = {url: occurrences for url, occurrences in url_occurrences.items() if len(occurrences) > 1}
    
    # Report statistics
    print(f"📊 STATISTICS:")
    print(f"   Files scanned: {total_files}")
    print(f"   Total rows: {total_rows}")
    print(f"   Total YouTube URLs found: {urls_found}")
    print(f"   Unique URLs: {len(url_occurrences)}")
    print(f"   Duplicate URLs: {len(duplicates)}")
    print(f"   Total duplicate entries: {sum(len(occurrences) for occurrences in duplicates.values())}")
    print(f"   Wasted entries: {sum(len(occurrences) - 1 for occurrences in duplicates.values())}\n")
    
    if not duplicates:
        print("✅ No duplicate URLs found!")
        return
    
    # Analyze patterns
    print(f"📋 DUPLICATE URL ANALYSIS:\n")
    
    # Group by duplication pattern
    same_church_duplicates = []
    cross_church_duplicates = []
    
    for url, occurrences in sorted(duplicates.items(), key=lambda x: -len(x[1])):
        churches = set(occ['church'] for occ in occurrences)
        
        if len(churches) == 1:
            same_church_duplicates.append((url, occurrences))
        else:
            cross_church_duplicates.append((url, occurrences))
    
    print(f"🔹 Same Church Duplicates: {len(same_church_duplicates)} URLs")
    print(f"🔹 Cross-Church Duplicates: {len(cross_church_duplicates)} URLs\n")
    
    # Show examples of each type
    if same_church_duplicates:
        print("=" * 80)
        print("SAME CHURCH DUPLICATES (URL appears multiple times in one church's CSV)")
        print("=" * 80)
        for i, (url, occurrences) in enumerate(same_church_duplicates[:5], 1):  # Show first 5
            print(f"\n{i}. URL: {url}")
            print(f"   Church: {occurrences[0]['church']}")
            print(f"   Occurrences: {len(occurrences)}")
            for occ in occurrences:
                print(f"      - Row {occ['row']}: {occ['date']} | {occ['title'][:50]} | Status: {occ['status']}")
        
        if len(same_church_duplicates) > 5:
            print(f"\n   ... and {len(same_church_duplicates) - 5} more same-church duplicates")
    
    if cross_church_duplicates:
        print("\n" + "=" * 80)
        print("CROSS-CHURCH DUPLICATES (URL appears in multiple churches' CSVs)")
        print("=" * 80)
        for i, (url, occurrences) in enumerate(cross_church_duplicates[:5], 1):  # Show first 5
            print(f"\n{i}. URL: {url}")
            print(f"   Appears in {len(set(occ['church'] for occ in occurrences))} churches:")
            for occ in occurrences:
                print(f"      - {occ['church']} (Row {occ['row']}): {occ['date']} | {occ['title'][:50]}")
        
        if len(cross_church_duplicates) > 5:
            print(f"\n   ... and {len(cross_church_duplicates) - 5} more cross-church duplicates")
    
    # Recommendations
    print("\n" + "=" * 80)
    print("🛠️  RECOMMENDATIONS TO PREVENT DUPLICATES")
    print("=" * 80)
    print("""
1. SAME-CHURCH DUPLICATES:
   - Cause: Re-scraping the same channel adds videos that already exist
   - Solution: In update_sermons.py, check if URL already exists in the CSV before adding
   - Add a deduplication step: Keep only the NEWEST entry (highest timestamp)

2. CROSS-CHURCH DUPLICATES:
   - Cause: Same video appears on multiple church channels (shared/reposted content)
   - Solution: In generate_site_data.py (ALREADY IMPLEMENTED):
     * Deduplication by videoUrl keeps only the NEWEST metadata
     * This ensures stale/archived copies don't corrupt the site

3. PREVENTION STRATEGY:
   - Before writing to CSV, check if URL already exists in that file
   - Add URL-based deduplication to update_sermons.py scraping logic
   - Consider a master URL registry to track which URLs have been processed
   - Use 'video_status' field to track removed/unavailable videos

4. DATA CLEANUP:
   - Run a one-time cleanup script to remove duplicate URLs from CSV files
   - Keep the entry with the most complete metadata (highest timestamp, most fields)
   - Back up CSVs before cleanup

5. MONITORING:
   - Run this audit script regularly (e.g., in daily_update.sh)
   - Alert when duplicate count increases
   - Log when update_sermons.py skips a duplicate URL
    """)

if __name__ == "__main__":
    audit_duplicate_urls()
