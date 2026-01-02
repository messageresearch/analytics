import os
import json
import re
import csv
import datetime
import zipfile
from pathlib import Path

# --- CONFIGURATION ---
DATA_DIR = "data"
OUTPUT_DIR = "site_api"
META_FILE = "metadata.json"
SEARCH_CHUNK_PREFIX = "text_chunk_"
CHUNK_SIZE_LIMIT = 5 * 1024 * 1024  # 5 MB
ZIP_FILENAME = "all_sermons_archive.zip"

# Regex Patterns
MENTION_REGEX = r"(?:brother\s+william|william|brother)\s+br[aeiou]n[dh]*[aeiou]m"
JESUS_REGEX = r"\bJesus\b" # Strict word boundary to avoid partial matches

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
        # --- Speaker Healing Logic ---
        SPEAKER_HEAL_LIST = set([
            "Eduan Naude", "Eduan Naudé", "Eduan Naud", "Forest Farmer The Fruit", "Forrest Farmer",
            "Financial Jubilee", "Finding Yourself", "Fitly Joined Together", "Five Comings"
        ])
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
        jesus_mentions = len(re.findall(JESUS_REGEX, content, re.IGNORECASE))
        word_count = len(content.split())
        
        rel_path = f"data/{church}/{filename}".replace(" ", "%20")

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
                "id": filename,
                "church": church,
                "date": date_str,
                "year": year,
                "timestamp": ts,
                "title": title,
                "speaker": speaker,
                "type": video_type,
                "language": language,
                "mentionCount": branham_mentions,
                "jesusCount": jesus_mentions,
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
                                # --- Speaker Healing Logic for summary/master CSVs ---
                                SPEAKER_HEAL_LIST = set([
                                    "Eduan Naude", "Eduan Naudé", "Eduan Naud", "Forest Farmer The Fruit", "Forrest Farmer",
                                    "Financial Jubilee", "Finding Yourself", "Fitly Joined Together", "Five Comings"
                                ])
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
    current_chunk = []
    current_chunk_size = 0
    chunk_index = 0
    
    # with zipfile.ZipFile(ZIP_FILENAME, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for church_folder in os.listdir(DATA_DIR):
        church_path = os.path.join(DATA_DIR, church_folder)
        if os.path.isdir(church_path):
            # Handle underscores in folder names for display
            church_display = church_folder.replace("_", " ")
            print(f"   Processing: {church_display}...")
            
            for filename in os.listdir(church_path):
                if filename.endswith(".txt"):
                    file_full_path = os.path.join(church_path, filename)
                    # zipf.write(file_full_path, arcname=f"{church_folder}/{filename}")
                    
                    data = parse_sermon(file_full_path, church_folder, filename) # Keep folder name for path
                    if data:
                        # Override display name in metadata
                        data['meta']['church'] = church_display
                        all_meta.append(data['meta'])
                        
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

    master_data = {
        "generated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "totalChunks": chunk_index,
        "sermons": sorted(all_meta, key=lambda x: x['timestamp'], reverse=True)
    }
    
    with open(os.path.join(OUTPUT_DIR, META_FILE), 'w', encoding='utf-8') as f:
        json.dump(master_data, f)
    
    # Generate slim metadata for faster initial load (~2MB vs ~14MB)
    # Removes: path (3.3MB), videoUrl (1MB) - reconstruct on-demand
    # Uses numeric index as id instead of full filename (saves ~2MB)
    slim_sermons = []
    id_to_full = {}  # Map numeric id to full sermon data for on-demand loading
    
    for idx, sermon in enumerate(master_data['sermons']):
        slim_sermons.append({
            "i": idx,  # numeric index as id
            "c": sermon['church'],
            "d": sermon['date'],
            "y": sermon['year'],
            "ts": sermon['timestamp'],
            "t": sermon['title'],
            "s": sermon['speaker'],
            "tp": sermon['type'],
            "l": sermon['language'],
            "m": sermon['mentionCount'],
            "j": sermon['jesusCount'],
            "w": sermon['wordCount'],
            "h": 1 if sermon['path'] else 0,  # hasTranscript boolean
        })
        id_to_full[idx] = {
            "id": sermon['id'],
            "path": sermon['path'],
            "videoUrl": sermon['videoUrl']
        }
    
    slim_data = {
        "generated": master_data['generated'],
        "totalChunks": master_data['totalChunks'],
        "sermons": slim_sermons
    }
    
    with open(os.path.join(OUTPUT_DIR, 'metadata_slim.json'), 'w', encoding='utf-8') as f:
        json.dump(slim_data, f, separators=(',', ':'))  # Compact JSON
    
    # Save id mapping for on-demand path/videoUrl lookup
    with open(os.path.join(OUTPUT_DIR, 'sermon_details.json'), 'w', encoding='utf-8') as f:
        json.dump(id_to_full, f)
    
    slim_size = os.path.getsize(os.path.join(OUTPUT_DIR, 'metadata_slim.json')) / 1024 / 1024
    full_size = os.path.getsize(os.path.join(OUTPUT_DIR, META_FILE)) / 1024 / 1024
    print(f"   ✅ Generated metadata_slim.json ({slim_size:.1f}MB vs {full_size:.1f}MB full)")

    # Generate available_churches.json - list of churches that have Summary CSVs
    available_churches = []
    transcript_summary_counts = {}  # Pre-computed counts for faster client load
    
    for fname in os.listdir(DATA_DIR):
        if fname.endswith("_Summary.csv"):
            church_name = fname.rsplit('_Summary.csv', 1)[0]
            display_name = church_name.replace('_', ' ')
            
            # Store both normalized (with underscores) and display name (with spaces)
            available_churches.append({
                "normalized": church_name,
                "display": display_name
            })
            
            # Pre-compute transcript counts from CSV
            csv_path = os.path.join(DATA_DIR, fname)
            try:
                total = 0
                with_transcript = 0
                with open(csv_path, 'r', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    rows = list(reader)
                    if rows:
                        # Find status column
                        header = [c.strip().lower() for c in rows[0]]
                        status_idx = -1
                        for i, h in enumerate(header):
                            if 'status' in h:
                                status_idx = i
                                break
                        
                        # Count rows (excluding header)
                        for r in rows[1:]:
                            if not r or all(not c.strip() for c in r):
                                continue
                            total += 1
                            if status_idx >= 0 and status_idx < len(r):
                                if 'success' in r[status_idx].lower():
                                    with_transcript += 1
                
                counts = {
                    "total": total,
                    "withTranscript": with_transcript,
                    "withoutTranscript": total - with_transcript
                }
                # Store under multiple keys for easy lookup
                transcript_summary_counts[church_name] = counts
                transcript_summary_counts[display_name] = counts
            except Exception as e:
                print(f"   ⚠️  Could not parse {fname}: {e}")
    
    available_churches.sort(key=lambda x: x['display'])
    
    available_path = os.path.join(OUTPUT_DIR, 'available_churches.json')
    with open(available_path, 'w', encoding='utf-8') as f:
        json.dump(available_churches, f)
    print(f"   ✅ Generated {available_path} with {len(available_churches)} churches")
    
    # Save pre-computed transcript summary counts (eliminates 37 CSV fetches on client)
    counts_path = os.path.join(OUTPUT_DIR, 'transcript_counts.json')
    with open(counts_path, 'w', encoding='utf-8') as f:
        json.dump(transcript_summary_counts, f)
    print(f"   ✅ Generated {counts_path} with {len(available_churches)} churches")

    # Copy data/ and site_api/ to docs/ for GitHub Pages deployment
    import shutil
    DOCS_DIR = "docs"
    print(f"\n   Syncing to {DOCS_DIR}/ for GitHub Pages...")
    
    # Sync data folder
    docs_data = os.path.join(DOCS_DIR, DATA_DIR)
    if os.path.exists(docs_data):
        shutil.rmtree(docs_data)
    shutil.copytree(DATA_DIR, docs_data)
    print(f"   ✅ Copied {DATA_DIR}/ → {docs_data}/")
    
    # Sync site_api folder
    docs_api = os.path.join(DOCS_DIR, OUTPUT_DIR)
    if os.path.exists(docs_api):
        shutil.rmtree(docs_api)
    shutil.copytree(OUTPUT_DIR, docs_api)
    print(f"   ✅ Copied {OUTPUT_DIR}/ → {docs_api}/")
    
    # Copy channels.json to docs/site_api/ for faster loading (avoids 404 fallback)
    channels_src = "channels.json"
    channels_dst = os.path.join(docs_api, "channels.json")
    if os.path.exists(channels_src):
        shutil.copy2(channels_src, channels_dst)
        print(f"   ✅ Copied {channels_src} → {channels_dst}")

    print("\n✅ GENERATION COMPLETE")

if __name__ == "__main__":
    main()