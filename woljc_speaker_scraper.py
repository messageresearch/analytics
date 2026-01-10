#!/usr/bin/env python3
"""
WOLJC Speaker Scraper
Scrapes the Word of Life Church website to retrieve speaker names for sermons,
then updates the summary CSV and transcript files.

Usage:
    python woljc_speaker_scraper.py [--dry-run] [--year YEAR]
    
Options:
    --dry-run    Preview changes without making them
    --year YEAR  Only scrape a specific year (e.g., 2024)
"""

import os
import re
import csv
import argparse
import time
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from difflib import SequenceMatcher

# Configuration
BASE_URL = "https://www.woljc.com"
SERMON_PAGES = {
    2026: "sermon-list"
    2025: "/2025-sermons/",
    2024: "/2024-sermons/",
    2023: "/2023-sermons/",
    2022: "/2022-sermon-list/",
    # Add more years as they become available
}

DATA_DIR = "data"
CHURCH_NAME = "Word_Of_Life_Church"
CSV_FILE = os.path.join(DATA_DIR, f"{CHURCH_NAME}_Summary.csv")
TRANSCRIPT_DIR = os.path.join(DATA_DIR, CHURCH_NAME)

# Minimum similarity threshold for title matching (0-1)
MATCH_THRESHOLD = 0.70


def parse_sermon_table_regex(html):
    """
    Parse sermon data from WOLJC HTML using regex.
    The website stores data in hidden <p> tags with specific CSS classes:
    - Sdate: Date in YYYY-MM-DD format
    - SdateFormat: Date in human-readable format
    - Stitle: Sermon title (also in data-title attribute)
    - Sspeaker: Speaker name
    """
    sermons = []
    
    # Find all table rows
    rows = re.findall(r'<tr[^>]*class="row-\d+"[^>]*>.*?</tr>', html, re.DOTALL | re.IGNORECASE)
    
    for row in rows:
        # Extract date (YYYY-MM-DD format from hidden Sdate)
        date_match = re.search(r"<p[^>]*class='Sdate'[^>]*>(\d{4}-\d{2}-\d{2})</p>", row)
        
        # Extract title from data-title attribute
        title_match = re.search(r"data-title='([^']+)'", row)
        if not title_match:
            # Fallback: try to get from Stitle content
            title_match = re.search(r"<p[^>]*class='Stitle'[^>]*>([^<]+)</p>", row)
        
        # Extract speaker
        speaker_match = re.search(r"<p[^>]*class='Sspeaker'[^>]*>([^<]+)</p>", row)
        
        if date_match and title_match and speaker_match:
            date_str = date_match.group(1)
            title = title_match.group(1).strip()
            speaker = speaker_match.group(1).strip()
            
            if date_str and title and speaker:
                sermons.append({
                    'date': date_str,  # Already in YYYY-MM-DD format
                    'title': title,
                    'speaker': speaker
                })
    
    return sermons


def clean_speaker_name(speaker):
    """Clean and normalize speaker name from website."""
    if not speaker:
        return ""
    
    # Remove "Bro." and "Sis." prefixes (handle missing space too like "Bro.Donny")
    speaker = re.sub(r'^(?:Bro\.?|Br\.?|Brother|Sis\.?|Sister)\s*', '', speaker, flags=re.IGNORECASE)
    
    # Remove trailing punctuation
    speaker = speaker.strip(" .,-;:")
    
    # Normalize known speakers
    speaker_lower = speaker.lower()
    if speaker_lower == 'darrell':
        return 'Darrell Ward'
    if 'donny reagan' in speaker_lower or speaker_lower == 'donny reagan':
        return 'Donny Reagan'
    
    # Skip if it's a church/group name
    skip_patterns = [
        r'\b(?:Church|Tabernacle|Ministry|Chapel|Choir|Congregation)\b',
        r'^Youth\s+', r'^Children',
    ]
    for pattern in skip_patterns:
        if re.search(pattern, speaker, re.IGNORECASE):
            return ""
    
    return speaker.strip()


def normalize_title(title):
    """Normalize title for comparison."""
    if not title:
        return ""
    
    title = title.lower()
    
    # Remove parenthetical suffixes like "(Youth Service)", "(Memorial Service)", etc.
    title = re.sub(r'\s*\([^)]*(?:service|meeting|sermon|message|youth|memorial|wedding|funeral)[^)]*\)', '', title, flags=re.IGNORECASE)
    
    # Remove part numbers variations for matching
    title = re.sub(r'\s*[\(\[]?(?:pt\.?|part)\s*\.?\s*\d+[\)\]]?', '', title)
    
    # Remove special characters (including apostrophes for normalization)
    title = re.sub(r"[`'\"''""–—.,!?;:-]", '', title)
    
    # Normalize whitespace
    title = re.sub(r'\s+', ' ', title).strip()
    
    # Normalize honorific prefixes: sis/sister, bro/brother
    title = re.sub(r'\b(?:sis|sister)\b', 'sister', title)
    title = re.sub(r'\b(?:bro|brother)\b', 'brother', title)
    
    # Normalize funeral/memorial/tribute terms (they often refer to the same service)
    title = re.sub(r'\b(?:funeral|tribute|memorial)\s*(?:service)?', 'memorial', title)
    
    # Strip leading articles (the, a, an)
    title = re.sub(r'^(?:the|a|an)\s+', '', title)
    
    # Normalize common sermon series names (after stripping articles and apostrophes)
    # "brides bill of rights" or "bride bill of rights" -> "brides bill of rights"
    title = re.sub(r'^brides?\s+bill\s+of\s+rights?', 'brides bill of rights', title)
    title = re.sub(r'^getting\s+in\s+(?:the\s+)?spirit', 'getting in the spirit', title)
    title = re.sub(r'^church\b', 'the church', title)
    title = re.sub(r'^brides?\s+dowry', 'brides dowry', title)
    title = re.sub(r'^godliness', 'godliness', title)
    
    return title


def extract_part_number(title):
    """Extract part number from title if present."""
    match = re.search(r'(?:pt\.?|part)\s*\.?\s*(\d+)', title, re.IGNORECASE)
    return int(match.group(1)) if match else None


def similarity_score(s1, s2):
    """Calculate similarity score between two strings."""
    return SequenceMatcher(None, s1.lower(), s2.lower()).ratio()


def parse_website_date(date_str):
    """Parse date from website format to YYYY-MM-DD."""
    # "Sunday, December 31, 2023" -> "2023-12-31"
    try:
        # Remove day of week
        date_str = re.sub(r'^(?:Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),?\s*', '', date_str)
        
        # Parse remaining date
        dt = datetime.strptime(date_str.strip(), "%B %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        try:
            dt = datetime.strptime(date_str.strip(), "%B %d %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None


def fetch_sermon_page(url, page=1):
    """Fetch sermon page content with pagination support."""
    full_url = f"{BASE_URL}{url}"
    if page > 1:
        full_url = f"{full_url}?tablepress-1_paged={page}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }
    
    try:
        req = Request(full_url, headers=headers)
        with urlopen(req, timeout=30) as response:
            return response.read().decode('utf-8')
    except (URLError, HTTPError) as e:
        print(f"  Error fetching {full_url}: {e}")
        return None


def scrape_year(year, url):
    """Scrape all sermons from a specific year's page."""
    print(f"Scraping {year} sermons from {url}...")
    
    html = fetch_sermon_page(url)
    if not html:
        return []
    
    # Use regex parser for WOLJC's specific HTML structure
    sermons = parse_sermon_table_regex(html)
    
    print(f"  Found {len(sermons)} sermons")
    return sermons


def dates_within_tolerance(date1, date2, tolerance_days=1):
    """Check if two YYYY-MM-DD dates are within tolerance_days of each other."""
    if not date1 or not date2:
        return False
    try:
        d1 = datetime.strptime(date1, "%Y-%m-%d")
        d2 = datetime.strptime(date2, "%Y-%m-%d")
        return abs((d1 - d2).days) <= tolerance_days
    except ValueError:
        return False


def find_best_match(web_sermon, csv_rows, require_date_match=True, date_tolerance_days=1):
    """Find the best matching CSV row for a web sermon.
    
    Args:
        web_sermon: Dict with 'date', 'title', 'speaker' from website
        csv_rows: List of CSV row dicts
        require_date_match: If True, only match if dates are within tolerance (default: True)
        date_tolerance_days: Number of days tolerance for date matching (default: 1)
    """
    web_title_norm = normalize_title(web_sermon['title'])
    web_date = web_sermon['date']  # Already in YYYY-MM-DD format
    web_part = extract_part_number(web_sermon['title'])
    
    best_match = None
    best_score = 0
    
    for row in csv_rows:
        # Skip if already has a speaker
        if row['speaker'] and row['speaker'] != "Unknown Speaker":
            continue
        
        # Require date match (with tolerance) if enabled
        if require_date_match and web_date:
            if not dates_within_tolerance(web_date, row['date'], date_tolerance_days):
                continue
            
        csv_title_norm = normalize_title(row['title'])
        csv_part = extract_part_number(row['title'])
        
        # Calculate base similarity
        score = similarity_score(web_title_norm, csv_title_norm)
        
        # Boost score if dates match exactly
        if web_date and row['date'] == web_date:
            score += 0.2
        
        # Boost score if part numbers match
        if web_part is not None and csv_part is not None:
            if web_part == csv_part:
                score += 0.15
            else:
                score -= 0.3  # Penalty for mismatched parts
        
        if score > best_score and score >= MATCH_THRESHOLD:
            best_score = score
            best_match = (row, score)
    
    return best_match


def load_csv():
    """Load the summary CSV file."""
    rows = []
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def save_csv(rows):
    """Save the updated CSV file."""
    fieldnames = ['date', 'status', 'speaker', 'title', 'url', 'last_checked', 'language', 'type', 'description']
    with open(CSV_FILE, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def find_transcript_file(date, title, old_speaker):
    """Find the transcript file that matches the given sermon."""
    if not os.path.exists(TRANSCRIPT_DIR):
        return None
    
    # Try to find exact match first
    for filename in os.listdir(TRANSCRIPT_DIR):
        if not filename.endswith('.txt'):
            continue
            
        # Parse filename: "YYYY-MM-DD - Title - Speaker.txt"
        if filename.startswith(date):
            # Check if title is in filename
            title_clean = re.sub(r'[^\w\s]', '', title.lower())
            filename_clean = re.sub(r'[^\w\s]', '', filename.lower())
            
            if title_clean[:30] in filename_clean or similarity_score(title_clean, filename_clean) > 0.5:
                return filename
    
    return None


def update_transcript_speaker_header(filepath, new_speaker):
    """
    Update the Speaker: line in the transcript file's internal header.
    This ensures the internal metadata matches the filename.
    
    Returns:
        bool: True if updated, False if no update needed or error
    """
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Check if file has a Speaker: line in the header (first 500 chars typically)
        header_match = re.search(r'^(Speaker:\s*)(.+)$', content, re.MULTILINE)
        
        if header_match:
            old_speaker = header_match.group(2).strip()
            # Only update if different
            if old_speaker != new_speaker:
                # Replace the Speaker: line
                new_content = re.sub(
                    r'^(Speaker:\s*)(.+)$',
                    f'Speaker: {new_speaker}',
                    content,
                    count=1,
                    flags=re.MULTILINE
                )
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                return True
        return False
    except Exception as e:
        print(f"  Warning: Could not update header in {filepath}: {e}")
        return False


def rename_transcript_file(old_filename, date, title, new_speaker):
    """Rename transcript file with new speaker name."""
    if not old_filename:
        return None
        
    old_path = os.path.join(TRANSCRIPT_DIR, old_filename)
    if not os.path.exists(old_path):
        return None
    
    # Clean title for filename
    title_clean = re.sub(r'[<>:"/\\|?*]', '', title)
    title_clean = re.sub(r'\s+', ' ', title_clean).strip()
    
    # Clean speaker for filename
    speaker_clean = re.sub(r'[<>:"/\\|?*]', '', new_speaker)
    speaker_clean = re.sub(r'\s+', ' ', speaker_clean).strip()
    
    new_filename = f"{date} - {title_clean} - {speaker_clean}.txt"
    new_path = os.path.join(TRANSCRIPT_DIR, new_filename)
    
    if old_path != new_path:
        return (old_path, new_path, old_filename, new_filename, new_speaker)
    return None


# Known sermon series patterns and their regex patterns
SERIES_PATTERNS = [
    (r"(?:the\s+)?bride'?s?\s+bill\s+of\s+rights?\s*(?:pt\.?|part)?\s*\.?\s*\d*", "Bride's Bill of Rights"),
    (r"getting\s+in\s+(?:the\s+)?spir[it]+\s*(?:pt\.?|part)?\s*\.?\s*\d*", "Getting In The Spirit"),
    (r"(?:the\s+)?church\s*(?:pt\.?|part)?\s*\.?\s*\d*", "The Church"),
    (r"godliness\s*(?:pt\.?|part)?\s*\.?\s*\d*", "Godliness"),
    (r"(?:the\s+)?bride'?s?\s+dowry\s*(?:pt\.?|part)?\s*\.?\s*\d*", "Bride's Dowry"),
    (r"youth\s+q\s*&?\s*a\s*(?:pt\.?|part)?\s*\.?\s*\d*", "Youth Q&A"),
    (r"what\s+is\s+the\s+holy\s+ghost\s*(?:pt\.?|part)?\s*\.?\s*\d*", "What Is The Holy Ghost"),
]


def identify_series(title):
    """Identify if a title belongs to a known sermon series."""
    if not title:
        return None
    
    title_lower = title.lower()
    # Remove special characters for matching
    title_clean = re.sub(r"[`'\"''""–—.,!?;:-]", '', title_lower)
    
    for pattern, series_name in SERIES_PATTERNS:
        if re.search(pattern, title_clean, re.IGNORECASE):
            return series_name
    
    return None


def infer_speakers_from_series(csv_rows):
    """
    Infer speakers for Unknown Speaker entries based on known series patterns.
    If most entries in a series have a known speaker, apply that to unknown entries.
    
    Returns:
        list: Updates made [(row, old_speaker, new_speaker, series_name), ...]
    """
    # Build series -> speaker mapping from known entries
    series_speakers = {}  # series_name -> {speaker: count}
    
    for row in csv_rows:
        speaker = row.get('speaker', '')
        if speaker and speaker != "Unknown Speaker":
            series = identify_series(row.get('title', ''))
            if series:
                if series not in series_speakers:
                    series_speakers[series] = {}
                series_speakers[series][speaker] = series_speakers[series].get(speaker, 0) + 1
    
    # Determine dominant speaker for each series (must have at least 3 entries and 80%+ share)
    series_dominant = {}
    for series, speakers in series_speakers.items():
        if not speakers:
            continue
        total = sum(speakers.values())
        if total < 3:
            continue
        # Find the most common speaker
        dominant = max(speakers.items(), key=lambda x: x[1])
        speaker_name, count = dominant
        # Must have at least 80% of entries
        if count / total >= 0.8:
            series_dominant[series] = speaker_name
    
    # Apply inferred speakers to Unknown Speaker entries
    updates = []
    for row in csv_rows:
        if row.get('speaker') != "Unknown Speaker":
            continue
        
        series = identify_series(row.get('title', ''))
        if series and series in series_dominant:
            inferred_speaker = series_dominant[series]
            updates.append((row, "Unknown Speaker", inferred_speaker, series))
            row['speaker'] = inferred_speaker
    
    return updates, series_dominant


def main():
    parser = argparse.ArgumentParser(description='Scrape WOLJC website for speaker names')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without making them')
    parser.add_argument('--year', type=int, help='Only scrape a specific year')
    args = parser.parse_args()
    
    print("=" * 60)
    print("WOLJC Speaker Scraper")
    print("=" * 60)
    
    if args.dry_run:
        print("*** DRY RUN MODE - No changes will be made ***\n")
    
    # Load existing CSV data
    print(f"Loading CSV: {CSV_FILE}")
    csv_rows = load_csv()
    unknown_count = sum(1 for r in csv_rows if r['speaker'] == "Unknown Speaker")
    print(f"Found {len(csv_rows)} total sermons, {unknown_count} with Unknown Speaker\n")
    
    # Scrape website
    all_web_sermons = []
    years_to_scrape = [args.year] if args.year else sorted(SERMON_PAGES.keys(), reverse=True)
    
    for year in years_to_scrape:
        if year not in SERMON_PAGES:
            print(f"No URL configured for year {year}")
            continue
        sermons = scrape_year(year, SERMON_PAGES[year])
        all_web_sermons.extend(sermons)
    
    print(f"\nTotal scraped sermons: {len(all_web_sermons)}")
    
    # Match and update
    print("\nMatching sermons...")
    updates = []
    file_renames = []
    
    for web_sermon in all_web_sermons:
        speaker = clean_speaker_name(web_sermon['speaker'])
        if not speaker:
            continue
            
        match = find_best_match(web_sermon, csv_rows)
        if match:
            row, score = match
            old_speaker = row['speaker']
            
            # Update the row in place
            row['speaker'] = speaker
            
            updates.append({
                'date': row['date'],
                'title': row['title'],
                'web_title': web_sermon['title'],
                'old_speaker': old_speaker,
                'new_speaker': speaker,
                'score': score
            })
            
            # Find and prepare file rename
            old_file = find_transcript_file(row['date'], row['title'], old_speaker)
            rename_info = rename_transcript_file(old_file, row['date'], row['title'], speaker)
            if rename_info:
                file_renames.append(rename_info)
    
    # Report web scraping results
    print(f"\nFound {len(updates)} matches from website:\n")
    
    for u in updates[:30]:  # Show first 30
        print(f"  [{u['score']:.2f}] {u['date']} - {u['title'][:40]}")
        print(f"         {u['old_speaker']} -> {u['new_speaker']}")
    
    if len(updates) > 30:
        print(f"  ... and {len(updates) - 30} more")
    
    # --- SERIES INFERENCE: Fill in remaining unknowns based on series patterns ---
    print("\n--- Series Pattern Inference ---")
    series_updates, series_dominant = infer_speakers_from_series(csv_rows)
    
    if series_dominant:
        print(f"Detected dominant speakers for {len(series_dominant)} series:")
        for series, speaker in series_dominant.items():
            print(f"  • {series} -> {speaker}")
    
    if series_updates:
        print(f"\nInferred {len(series_updates)} additional speakers from series patterns:")
        for row, old_speaker, new_speaker, series in series_updates[:15]:
            print(f"  {row['date']} - {row['title'][:35]} -> {new_speaker} (via {series})")
        if len(series_updates) > 15:
            print(f"  ... and {len(series_updates) - 15} more")
        
        # Add file renames for series-inferred updates
        for row, old_speaker, new_speaker, series in series_updates:
            old_file = find_transcript_file(row['date'], row['title'], old_speaker)
            rename_info = rename_transcript_file(old_file, row['date'], row['title'], new_speaker)
            if rename_info:
                file_renames.append(rename_info)
    else:
        print("No additional speakers could be inferred from series patterns.")
    
    total_updates = len(updates) + len(series_updates)
    print(f"\nTotal updates: {total_updates} ({len(updates)} from web + {len(series_updates)} from series)")
    print(f"File renames queued: {len(file_renames)}")
    
    # Apply changes
    if not args.dry_run and total_updates > 0:
        print("\nApplying changes...")
        
        # Save CSV
        save_csv(csv_rows)
        print(f"  Updated {CSV_FILE}")
        
        # Rename files AND update internal headers
        renamed = 0
        headers_updated = 0
        for old_path, new_path, old_name, new_name, new_speaker in file_renames:
            try:
                # CRITICAL: Update the internal Speaker: header BEFORE renaming
                if update_transcript_speaker_header(old_path, new_speaker):
                    headers_updated += 1
                
                # Then rename the file
                os.rename(old_path, new_path)
                renamed += 1
            except OSError as e:
                print(f"  Error renaming {old_name}: {e}")
        
        print(f"  Renamed {renamed} transcript files")
        print(f"  Updated {headers_updated} internal Speaker: headers")
        
        # Final count
        new_unknown = sum(1 for r in csv_rows if r['speaker'] == "Unknown Speaker")
        print(f"\nRemaining Unknown Speaker entries: {new_unknown}")
        print(f"Updated: {unknown_count - new_unknown} entries")
    
    print("\nDone!")


def update_speakers_for_videos(video_entries, dry_run=False):
    """
    Update speaker names for specific video entries by matching against WOLJC website.
    
    This function is designed to be called from update_sermons.py as post-processing
    for newly added Word of Life Church videos.
    
    Args:
        video_entries: List of dicts with 'date', 'title', 'speaker' keys
                      (entries with speaker != "Unknown Speaker" are skipped)
        dry_run: If True, preview changes without applying them
        
    Returns:
        dict: Statistics about updates made
            - 'updated': number of entries updated
            - 'matches': list of (date, title, old_speaker, new_speaker)
    """
    if not video_entries:
        return {'updated': 0, 'matches': []}
    
    # Filter to only Unknown Speaker entries
    unknown_entries = [e for e in video_entries 
                       if e.get('speaker') == "Unknown Speaker" or not e.get('speaker')]
    
    if not unknown_entries:
        return {'updated': 0, 'matches': []}
    
    print(f"\n   🔍 WOLJC Speaker Lookup: Checking {len(unknown_entries)} entries...")
    
    # Determine which years to scrape based on video dates
    years_needed = set()
    for entry in unknown_entries:
        try:
            year = int(entry.get('date', '')[:4])
            if year in SERMON_PAGES:
                years_needed.add(year)
        except (ValueError, TypeError):
            pass
    
    if not years_needed:
        # Default to current year if no dates parseable
        current_year = datetime.now().year
        if current_year in SERMON_PAGES:
            years_needed.add(current_year)
    
    # Scrape only the needed years from website
    all_web_sermons = []
    for year in sorted(years_needed, reverse=True):
        if year in SERMON_PAGES:
            html = fetch_sermon_page(SERMON_PAGES[year])
            if html:
                sermons = parse_sermon_table_regex(html)
                all_web_sermons.extend(sermons)
    
    if not all_web_sermons:
        print(f"   ⚠️ Could not fetch WOLJC website data")
        return {'updated': 0, 'matches': []}
    
    # Match unknown entries against web sermons
    matches = []
    for entry in unknown_entries:
        entry_date = entry.get('date', '')
        entry_title = entry.get('title', '')
        
        # Create a pseudo web_sermon to search for
        best_web_match = None
        best_score = 0
        
        for web in all_web_sermons:
            speaker = clean_speaker_name(web['speaker'])
            if not speaker:
                continue
            
            # Check date tolerance
            if not dates_within_tolerance(web['date'], entry_date, 1):
                continue
            
            # Calculate title similarity
            web_title_norm = normalize_title(web['title'])
            entry_title_norm = normalize_title(entry_title)
            score = similarity_score(web_title_norm, entry_title_norm)
            
            # Boost for exact date match
            if web['date'] == entry_date:
                score += 0.2
            
            # Check part numbers
            web_part = extract_part_number(web['title'])
            entry_part = extract_part_number(entry_title)
            if web_part is not None and entry_part is not None:
                if web_part == entry_part:
                    score += 0.15
                else:
                    score -= 0.3
            
            if score > best_score and score >= MATCH_THRESHOLD:
                best_score = score
                best_web_match = (web, speaker, score)
        
        if best_web_match:
            web, speaker, score = best_web_match
            matches.append({
                'date': entry_date,
                'title': entry_title,
                'old_speaker': entry.get('speaker', 'Unknown Speaker'),
                'new_speaker': speaker,
                'score': score
            })
            # Update the entry in place
            if not dry_run:
                entry['speaker'] = speaker
    
    if matches:
        print(f"   ✅ Found {len(matches)} speaker matches from WOLJC website:")
        for m in matches[:5]:
            print(f"      [{m['score']:.2f}] {m['date']} - {m['title'][:35]} -> {m['new_speaker']}")
        if len(matches) > 5:
            print(f"      ... and {len(matches) - 5} more")
    
    return {
        'updated': len(matches),
        'matches': matches
    }


if __name__ == "__main__":
    main()

