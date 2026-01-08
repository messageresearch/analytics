import os
import json
import re
import csv
import datetime
import zipfile
import hashlib
from pathlib import Path

# --- SHARED CONFIGURATION ---
# Try to load from centralized config module (new approach)
# Falls back to hardcoded values if config module is unavailable
try:
    from config import shared_config
    _USE_SHARED_CONFIG = True
except ImportError:
    _USE_SHARED_CONFIG = False
    print("⚠️  Config module not found, using local defaults")

# --- CONFIGURATION ---
DATA_DIR = "data"
OUTPUT_DIR = "site_api"
META_FILE = "metadata.json"
SEARCH_CHUNK_PREFIX = "text_chunk_"
ZIP_FILENAME = "all_sermons_archive.zip"

# Load from shared config if available, otherwise use hardcoded defaults
if _USE_SHARED_CONFIG:
    CHUNK_SIZE_LIMIT = shared_config.get_chunk_size_limit()
    MENTION_REGEX = shared_config.get_mention_regex()
    DEFAULT_REGEX = shared_config.get_default_search_regex()
    SPEAKER_HEAL_LIST = shared_config.get_invalid_speakers()
else:
    CHUNK_SIZE_LIMIT = 5 * 1024 * 1024  # 5 MB
    MENTION_REGEX = r"(?:brother\s+william|william|brother)\s+br[aeiou]n[dh]*[aeiou]m"
    DEFAULT_REGEX = r"\b(?:(?:brother\s+william)|william|brother)\s+br[aeiou]n[dh]*[aeiou]m\b"
    SPEAKER_HEAL_LIST = {
        "Eduan Naude", "Eduan Naudé", "Eduan Naud", "Forest Farmer The Fruit", "Forrest Farmer",
        "Financial Jubilee", "Finding Yourself", "Fitly Joined Together", "Five Comings"
    }

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def normalize_key(s):
    if not s: return ''
    return re.sub(r'[^0-9a-z]+', '', s.lower())


def extract_first_youtube(text):
    if not text: return ''
    m = re.search(r"https?://(?:www\.)?(?:youtube\.com/watch\?v=[A-Za-z0-9_\-]+|youtu\.be/[A-Za-z0-9_\-]+)", text)
    if m:
        return m.group(0)
    # Look for lines like 'URL: https://...'
    for line in text.splitlines():
        if 'youtube.com/watch' in line or 'youtu.be/' in line:
            mm = re.search(r"https?://\S+", line)
            if mm: return mm.group(0)
    return ''


def extract_youtube_video_id(url: str) -> str:
    if not url:
        return ''
    # Common formats:
    # - https://www.youtube.com/watch?v=VIDEOID
    # - https://youtu.be/VIDEOID
    # - https://www.youtube.com/shorts/VIDEOID
    m = re.search(r"(?:v=|youtu\.be/|/shorts/)([A-Za-z0-9_\-]{6,})", url)
    if not m:
        return ''
    return m.group(1)


def stable_short_hash(text: str, length: int = 12) -> str:
    if text is None:
        text = ''
    return hashlib.sha1(text.encode('utf-8', errors='ignore')).hexdigest()[:length]


def parse_sermon(filepath, church, filename, summary_map=None):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Metadata Extraction
        date_match = re.search(r"Date:\s+(.+)", content)
        title_match = re.search(r"Title:\s+(.+)", content)
        speaker_match = re.search(r"Speaker:\s+(.+)", content)
        type_match = re.search(r"Type:\s+(.+)", content)
        lang_match = re.search(r"Language:\s*(.+)", content)
        
        date_str = date_match.group(1).strip() if date_match else "Unknown Date"
        title = title_match.group(1).strip() if title_match else "Unknown Title"
        speaker = speaker_match.group(1).strip() if speaker_match else "Unknown Speaker"
        # --- Speaker Healing Logic (uses centralized config) ---
        if speaker in SPEAKER_HEAL_LIST:
            speaker = "Unknown Speaker"
        video_type = type_match.group(1).strip() if type_match else "Full Sermon"
        language = lang_match.group(1).strip() if lang_match else "English"
        
        try:
            ts = datetime.datetime.strptime(date_str, "%Y-%m-%d").timestamp() * 1000
            year = date_str[:4]
        except:
            # Try to extract date from filename (common pattern: YYYY-MM-DD at start)
            fn_date = None
            mfn = re.search(r"(19|20)\d{2}[-_]\d{2}[-_]\d{2}", filename)
            if mfn:
                fn_date = mfn.group(0).replace('_', '-').replace('.', '-')
            try:
                if fn_date:
                    ts = datetime.datetime.strptime(fn_date, "%Y-%m-%d").timestamp() * 1000
                    year = fn_date[:4]
                else:
                    ts = 0
                    year = "Unknown"
            except:
                ts = 0
                year = "Unknown"

        # Counts
        branham_mentions = len(re.findall(MENTION_REGEX, content, re.IGNORECASE))
        word_count = len(content.split())
        
        # URL-encode special characters that break GitHub Pages URLs
        # Note: # must be encoded as %23 or it gets treated as URL fragment
        rel_path = f"data/{church}/{filename}".replace(" ", "%20").replace("#", "%23")

        # Try to find a YouTube URL inside the transcript first
        video_url = extract_first_youtube(content)

        # If not found, try the summary_map (keyed by normalized title+date)
        if (not video_url) and summary_map is not None:
            key = normalize_key(title) + '|' + normalize_key(date_str)
            if key in summary_map:
                video_url = summary_map[key]
            else:
                # fallback: try using only normalized title
                tkey = normalize_key(title)
                if tkey in summary_map:
                    video_url = summary_map[tkey]

        return {
            "meta": {
                "id": rel_path,
                "church": church,
                "date": date_str,
                "year": year,
                "timestamp": ts,
                "title": title,
                "speaker": speaker,
                "type": video_type,
                "language": language,
                "mentionCount": branham_mentions,
                "wordCount": word_count,
                "path": rel_path,
                "videoUrl": video_url or ""
            },
            "text": content
        }
    except Exception as e:
        print(f"Skipping {filename}: {e}")
        return None

def cleanup_old_chunks():
    """Remove all existing text_chunk_*.json files before regenerating."""
    import glob
    pattern = os.path.join(OUTPUT_DIR, f"{SEARCH_CHUNK_PREFIX}*.json")
    old_chunks = glob.glob(pattern)
    if old_chunks:
        print(f"   Cleaning up {len(old_chunks)} old text chunk files...")
        for chunk_file in old_chunks:
            try:
                os.remove(chunk_file)
            except Exception as e:
                print(f"   Warning: Could not remove {chunk_file}: {e}")

def collect_default_matches(all_texts):
    """
    Collect all unique matched terms from the default regex across all sermon texts.
    Returns a dict of {lowercase_term: count} for the "Terms Found in Results" feature.
    This pre-computation allows the site to load instantly without scanning all chunks.
    """
    term_counts = {}
    compiled_regex = re.compile(DEFAULT_REGEX, re.IGNORECASE)
    
    for text in all_texts:
        matches = compiled_regex.findall(text)
        for match in matches:
            # Normalize: lowercase and collapse whitespace
            normalized = ' '.join(match.lower().split())
            term_counts[normalized] = term_counts.get(normalized, 0) + 1
    
    # Sort by count (descending) for consistent output
    sorted_terms = sorted(term_counts.items(), key=lambda x: (-x[1], x[0]))
    return sorted_terms

def main():
    print("--- 🚀 GENERATING SITE DATA v4 (With videoUrl index) ---")
    ensure_dir(OUTPUT_DIR)
    cleanup_old_chunks()
    # Build summary CSV index: map normalized(title)|normalized(date) -> youtube URL
    summary_map = {}
    print("   Building summary CSV index...")
    for fname in os.listdir(DATA_DIR):
        if fname.endswith("_Summary.csv"):
            path = os.path.join(DATA_DIR, fname)
            try:
                with open(path, 'r', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    for row in reader:
                        if not row: continue
                        # try to find a youtube url in the row
                        url = ''
                        for cell in row:
                            if cell and ("youtube.com/watch" in cell or "youtu.be/" in cell):
                                url = cell.strip()
                                break
                        # try to extract date (YYYY-MM-DD) and title
                        date_str = ''
                        title = ''
                        for cell in row:
                            if re.match(r"^(19|20)\d{2}-\d{2}-\d{2}$", cell.strip()):
                                date_str = cell.strip(); break
                        # If standard positions present, try common columns
                        if len(row) > 3 and not title:
                            title = row[3].strip()
                        # fallback: pick the longest non-url, non-date field
                        if not title:
                            candidates = [c.strip() for c in row if c and 'youtube.com' not in c and 'youtu.be' not in c and not re.match(r"^(19|20)\d{2}-\d{2}-\d{2}$", c.strip())]
                            if candidates:
                                title = max(candidates, key=lambda s: len(s))
                        if url and title:
                            k1 = normalize_key(title) + '|' + normalize_key(date_str)
                            k2 = normalize_key(title)
                            summary_map[k1] = url
                            if k2 not in summary_map:
                                summary_map[k2] = url
            except Exception as e:
                print(f"Failed to read summary CSV {path}: {e}")

    # Now build a master CSV directly from the summary files (faster than scanning transcripts)
    try:
        master_rows = []
        seen_ids = set()  # Track seen IDs to prevent duplicates
        header_written = False
        master_path = os.path.join(OUTPUT_DIR, 'master_sermons.csv')
        print(f"   Building master CSV at {master_path}...")
        for fname in os.listdir(DATA_DIR):
            if fname.endswith("_Summary.csv"):
                path = os.path.join(DATA_DIR, fname)
                church_name = fname.rsplit('_Summary.csv', 1)[0].replace('_', ' ')
                try:
                    with open(path, 'r', encoding='utf-8') as csvfile:
                        reader = csv.reader(csvfile)
                        # Try to detect header row
                        rows = list(reader)
                        start_row = 0
                        if rows:
                            first = [c.strip().lower() for c in rows[0]]
                            if any(k in first for k in ('date','title','url','speaker')):
                                start_row = 1
                        for r in rows[start_row:]:
                            if not r or all(not c.strip() for c in r):
                                continue
                            # find youtube url in row
                            video = ''
                            for cell in r:
                                if cell and ("youtube.com/watch" in cell or "youtu.be/" in cell):
                                    video = cell.strip(); break
                            # guess date, title, speaker, type, language, description
                            date = ''
                            title = ''
                            speaker = ''
                            vtype = ''
                            lang = ''
                            desc = ''
                            # heuristics: common layout: date, status, speaker, title, url, generated, language, type, description
                            if len(r) >= 4:
                                date = r[0].strip()
                                speaker = r[2].strip() if len(r) > 2 else ''
                                # --- Speaker Healing Logic (uses centralized config) ---
                                if speaker in SPEAKER_HEAL_LIST:
                                    speaker = "Unknown Speaker"
                                title = r[3].strip() if len(r) > 3 else ''
                            # try to get language/type/description if present
                            if len(r) > 6:
                                lang = r[6].strip()
                            if len(r) > 7:
                                vtype = r[7].strip()
                            if len(r) > 8:
                                desc = r[8].strip()
                            # fallback: detect date-like cell anywhere
                            if not date:
                                for cell in r:
                                    if re.match(r"^(19|20)\d{2}-\d{2}-\d{2}$", cell.strip()):
                                        date = cell.strip(); break
                            # fallback: find longest non-url cell as title
                            if not title:
                                candidates = [c.strip() for c in r if c and 'youtube.com' not in c and 'youtu.be' not in c and not re.match(r"^(19|20)\d{2}-\d{2}-\d{2}$", c.strip())]
                                if candidates:
                                    title = max(candidates, key=lambda s: len(s))
                            # final id
                            uid = normalize_key(church_name) + '_' + normalize_key(title) + '_' + (date.replace('-', '') if date else '')
                            # Skip duplicates (same id)
                            if uid in seen_ids:
                                continue
                            seen_ids.add(uid)
                            master_rows.append([uid, date, church_name, title, speaker, vtype, lang, video, desc])
                except Exception as e:
                    print(f"Failed to parse summary CSV {path}: {e}")
        # write master CSV
        if master_rows:
            with open(master_path, 'w', encoding='utf-8', newline='') as out:
                # BOM for Excel
                out.write('\ufeff')
                writer = csv.writer(out)
                writer.writerow(['id','date','church','title','speaker','type','language','videoUrl','description'])
                for row in master_rows:
                    writer.writerow(row)
            print(f"   Wrote master CSV with {len(master_rows)} rows")
        else:
            print("   No summary rows found to build master CSV")
    except Exception as e:
        print(f"Failed to build master CSV: {e}")
    
    all_meta = []
    all_texts = []  # Collect all sermon texts for default regex pre-computation
    current_chunk = []
    current_chunk_size = 0
    chunk_index = 0
    
    # Current year for filtering out invalid future dates
    current_year = datetime.datetime.now().year
    skipped_future = 0
    
    # Track which videos have transcripts (by normalized title|date key)
    transcripts_found = set()
    
    # with zipfile.ZipFile(ZIP_FILENAME, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for church_folder in os.listdir(DATA_DIR):
        # Skip William Branham Sermons - that's a separate archive handled by generate_wmb_site_data.py
        if church_folder == "William_Branham_Sermons":
            continue
        church_path = os.path.join(DATA_DIR, church_folder)
        if os.path.isdir(church_path):
            # Handle underscores in folder names for display
            church_display = church_folder.replace("_", " ")
            print(f"   Processing: {church_display}...")
            
            for filename in os.listdir(church_path):
                lower_name = filename.lower()
                if lower_name.endswith(".txt") and not lower_name.endswith(".timestamped.txt") and not lower_name.endswith(" - timestamped.txt"):
                    file_full_path = os.path.join(church_path, filename)
                    # zipf.write(file_full_path, arcname=f"{church_folder}/{filename}")
                    
                    data = parse_sermon(file_full_path, church_folder, filename) # Keep folder name for path
                    if data:
                        # Filter out invalid future years (data entry errors)
                        try:
                            sermon_year = int(data['meta']['year'])
                            if sermon_year > current_year:
                                skipped_future += 1
                                continue  # Skip this sermon
                        except (ValueError, TypeError):
                            pass  # Keep sermons with unknown year
                        
                        # Override display name in metadata
                        data['meta']['church'] = church_display
                        data['meta']['hasTranscript'] = True  # Mark as having transcript
                        all_meta.append(data['meta'])
                        all_texts.append(data['text'])  # Collect text for default regex matching
                        
                        # Track this video as having a transcript
                        track_key = normalize_key(data['meta']['title']) + '|' + normalize_key(data['meta']['date'])
                        transcripts_found.add(track_key)
                        
                        text_entry = {"id": data['meta']['id'], "text": data['text']}
                        text_size = len(data['text'].encode('utf-8'))
                        current_chunk.append(text_entry)
                        current_chunk_size += text_size
                        
                        if current_chunk_size >= CHUNK_SIZE_LIMIT:
                            with open(os.path.join(OUTPUT_DIR, f"{SEARCH_CHUNK_PREFIX}{chunk_index}.json"), 'w', encoding='utf-8') as f:
                                json.dump(current_chunk, f)
                            current_chunk = []
                            current_chunk_size = 0
                            chunk_index += 1

    if current_chunk:
        with open(os.path.join(OUTPUT_DIR, f"{SEARCH_CHUNK_PREFIX}{chunk_index}.json"), 'w', encoding='utf-8') as f:
            json.dump(current_chunk, f)
        chunk_index += 1

    # Now add entries for videos WITHOUT transcripts from CSV summaries
    print("   Adding videos without transcripts from CSV summaries...")
    no_transcript_count = 0
    no_transcript_seen_videos = set()  # normalized_church|video_id_or_url_hash
    for fname in os.listdir(DATA_DIR):
        if fname.endswith("_Summary.csv"):
            path = os.path.join(DATA_DIR, fname)
            church_name = fname.rsplit('_Summary.csv', 1)[0].replace('_', ' ')
            try:
                with open(path, 'r', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    rows = list(reader)
                    # Skip header
                    start_row = 0
                    if rows:
                        first = [c.strip().lower() for c in rows[0]]
                        if any(k in first for k in ('date','title','url','speaker','status')):
                            start_row = 1
                    for r in rows[start_row:]:
                        if not r or all(not c.strip() for c in r):
                            continue
                        # Check if status column contains "No Transcript"
                        status = r[1].strip() if len(r) > 1 else ''
                        if status != "No Transcript":
                            continue
                        # Parse the row
                        date_str = r[0].strip() if len(r) > 0 else ''
                        speaker = r[2].strip() if len(r) > 2 else ''
                        title = r[3].strip() if len(r) > 3 else ''
                        video_url = ''
                        for cell in r:
                            if cell and ("youtube.com/watch" in cell or "youtu.be/" in cell):
                                video_url = cell.strip()
                                break
                        lang = r[6].strip() if len(r) > 6 else ''
                        vtype = r[7].strip() if len(r) > 7 else ''
                        
                        # Skip if we already have a transcript for this
                        check_key = normalize_key(title) + '|' + normalize_key(date_str)
                        if check_key in transcripts_found:
                            continue

                        # Skip duplicates for the same YouTube video (often appears multiple times in *_Summary.csv)
                        video_id = extract_youtube_video_id(video_url)
                        video_key = video_id if video_id else stable_short_hash(video_url)
                        dedupe_key = normalize_key(church_name) + '|' + video_key
                        if dedupe_key in no_transcript_seen_videos:
                            continue
                        no_transcript_seen_videos.add(dedupe_key)
                        
                        # Parse date/year
                        year = "Unknown"
                        ts = 0
                        if date_str:
                            try:
                                dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                                year = str(dt.year)
                                ts = dt.timestamp() * 1000
                                # Skip future dates
                                if int(year) > current_year:
                                    continue
                            except:
                                pass
                        
                        # Heal speakers
                        if speaker in SPEAKER_HEAL_LIST:
                            speaker = "Unknown Speaker"
                        
                        # Include video_key so IDs are unique and stable across regenerations
                        entry_id = f"NO_TRANSCRIPT_{normalize_key(church_name)}_{date_str}_{normalize_key(title)[:30]}_{video_key}"
                        all_meta.append({
                            "id": entry_id,
                            "church": church_name,
                            "date": date_str,
                            "year": year,
                            "timestamp": ts,
                            "title": title,
                            "speaker": speaker,
                            "type": vtype or "Unknown",
                            "language": lang or "Unknown",
                            "mentionCount": 0,
                            "wordCount": 0,
                            "path": "",  # No transcript file
                            "videoUrl": video_url,
                            "hasTranscript": False
                        })
                        no_transcript_count += 1
            except Exception as e:
                print(f"   Warning: Failed to parse {path} for no-transcript entries: {e}")
    
    print(f"   Added {no_transcript_count} videos without transcripts")

    # Pre-compute default regex matches for instant page load
    print("   Pre-computing default regex matches...")
    default_matches = collect_default_matches(all_texts)
    print(f"   Found {len(default_matches)} unique term variations, {sum(c for _, c in default_matches)} total matches")
    
    if skipped_future > 0:
        print(f"   ⚠️  Skipped {skipped_future} sermons with invalid future dates (year > {current_year})")

    master_data = {
        "generated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "totalChunks": chunk_index,
        "totalSermons": len(all_meta),
        "sermons": sorted(all_meta, key=lambda x: x['timestamp'], reverse=True),
        "defaultSearchTerms": [{"term": term, "count": count} for term, count in default_matches]
    }
    
    with open(os.path.join(OUTPUT_DIR, META_FILE), 'w', encoding='utf-8') as f:
        json.dump(master_data, f)

    # Generate available_churches.json - list of churches that have Summary CSVs
    available_churches = []
    for fname in os.listdir(DATA_DIR):
        if fname.endswith("_Summary.csv"):
            church_name = fname.rsplit('_Summary.csv', 1)[0]
            # Store both normalized (with underscores) and display name (with spaces)
            available_churches.append({
                "normalized": church_name,
                "display": church_name.replace('_', ' ')
            })
    available_churches.sort(key=lambda x: x['display'])
    
    available_path = os.path.join(OUTPUT_DIR, 'available_churches.json')
    with open(available_path, 'w', encoding='utf-8') as f:
        json.dump(available_churches, f)
    print(f"   ✅ Generated {available_path} with {len(available_churches)} churches")

    # Copy data/ and site_api/ to docs/ for GitHub Pages deployment
    import shutil
    import subprocess
    DOCS_DIR = "docs"
    print(f"\n   Syncing to {DOCS_DIR}/ for GitHub Pages...")
    
    # Helper to robustly remove directory (handles iCloud sync issues)
    def robust_rmtree(path):
        if not os.path.exists(path):
            return
        try:
            shutil.rmtree(path)
        except OSError:
            # Fallback to rm -rf for stubborn iCloud-synced directories
            subprocess.run(['rm', '-rf', path], check=False)
    
    # Sync data folder
    docs_data = os.path.join(DOCS_DIR, DATA_DIR)
    robust_rmtree(docs_data)
    shutil.copytree(DATA_DIR, docs_data)
    print(f"   ✅ Copied {DATA_DIR}/ → {docs_data}/")
    
    # Sync site_api folder
    docs_api = os.path.join(DOCS_DIR, OUTPUT_DIR)
    robust_rmtree(docs_api)
    shutil.copytree(OUTPUT_DIR, docs_api)
    print(f"   ✅ Copied {OUTPUT_DIR}/ → {docs_api}/")

    print("\n✅ GENERATION COMPLETE")

if __name__ == "__main__":
    main()