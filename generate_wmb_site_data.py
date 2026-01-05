#!/usr/bin/env python3
"""
Generate site data for William Branham sermon archive.
Creates wmb_api/ folder with metadata.json and text_chunk_*.json files
following the same format as the main site's generate_site_data.py
"""

import os
import json
import re
import datetime
from pathlib import Path

# --- CONFIGURATION ---
DATA_DIR = "data/William_Branham_Sermons"
OUTPUT_DIR = "wmb_api"
META_FILE = "metadata.json"
SEARCH_CHUNK_PREFIX = "text_chunk_"

# 5 MB chunk size limit
CHUNK_SIZE_LIMIT = 5 * 1024 * 1024

# For topic detection - common themes in Branham's sermons
TOPIC_KEYWORDS = {
    "Seven Seals": ["seal", "seals", "revelation", "book sealed"],
    "Seven Church Ages": ["church age", "ephesus", "smyrna", "pergamos", "thyatira", "sardis", "philadelphia", "laodicea"],
    "Serpent's Seed": ["serpent", "serpent's seed", "cain", "abel"],
    "Divine Healing": ["healing", "healed", "miracle", "cancer", "tumor", "blind", "deaf"],
    "Bride": ["bride", "rapture", "caught away", "going home"],
    "End Time": ["end time", "last days", "tribulation", "millennium"],
    "Baptism": ["baptism", "water baptism", "jesus name", "trinity"],
    "Godhead": ["godhead", "trinity", "oneness", "father son holy ghost"],
    "Prophet": ["prophet", "prophets", "elijah", "moses", "vindication"],
    "Angel": ["angel", "pillar of fire", "light", "supernatural"],
    "Faith": ["faith", "believe", "believing", "doubt"],
    "Holy Ghost": ["holy ghost", "holy spirit", "baptism of the spirit", "filled"],
}

def ensure_dir(directory):
    """Create directory if it doesn't exist."""
    Path(directory).mkdir(parents=True, exist_ok=True)

def normalize_key(s):
    """Normalize string for comparison."""
    if not s: return ''
    return re.sub(r'[^0-9a-z]+', '', s.lower())

def extract_topics(text):
    """Extract detected topics from sermon text."""
    text_lower = text.lower()
    detected = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        for kw in keywords:
            if kw in text_lower:
                if topic not in detected:
                    detected.append(topic)
                break
    return detected

def parse_sermon(filepath, filename):
    """Parse a single sermon file and extract metadata + content."""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
        
        # Extract metadata from header
        date_match = re.search(r"Date:\s+(.+)", content)
        title_match = re.search(r"Title:\s+(.+)", content)
        venue_match = re.search(r"Venue:\s+(.+)", content)
        location_match = re.search(r"Location:\s+(.+)", content)
        duration_match = re.search(r"Duration:\s+(.+)", content)
        date_code_match = re.search(r"DateCode:\s+(.+)", content)
        
        date_str = date_match.group(1).strip() if date_match else "Unknown Date"
        title = title_match.group(1).strip() if title_match else filename.replace('.txt', '')
        venue = venue_match.group(1).strip() if venue_match else "Unknown Venue"
        location = location_match.group(1).strip() if location_match else "Unknown Location"
        duration_str = duration_match.group(1).strip() if duration_match else "Unknown"
        date_code = date_code_match.group(1).strip() if date_code_match else ""
        
        # Parse duration minutes
        duration_minutes = 0
        dur_match = re.search(r'(\d+)', duration_str)
        if dur_match:
            duration_minutes = int(dur_match.group(1))
        
        # Extract transcript text (after "--- TRANSCRIPT ---")
        transcript_marker = "--- TRANSCRIPT ---"
        if transcript_marker in content:
            transcript_text = content.split(transcript_marker, 1)[1].strip()
        else:
            # Fall back to full content minus metadata
            transcript_text = re.sub(r'^(Date|Title|Speaker|Type|Language|Venue|Location|Duration|Source|DateCode):.*$', '', content, flags=re.MULTILINE).strip()
        
        # Calculate timestamp
        try:
            ts = datetime.datetime.strptime(date_str, "%Y-%m-%d").timestamp() * 1000
            year = date_str[:4]
        except:
            ts = 0
            year = "Unknown"
        
        # Word count
        word_count = len(transcript_text.split())
        
        # Detect topics
        topics = extract_topics(transcript_text)
        
        # Living Word Broadcast index URL
        lwb_url = "https://www.livingwordbroadcast.org/wbtextindex"
        
        # Store the raw path - let the browser/fetch handle URL encoding
        rel_path = f"data/William_Branham_Sermons/{filename}"
        
        return {
            "meta": {
                "id": filename,
                "date": date_str,
                "year": year,
                "timestamp": ts,
                "title": title,
                "speaker": "William Branham",
                "type": "Sermon",
                "language": "English",
                "venue": venue,
                "location": location,
                "duration": duration_minutes,
                "dateCode": date_code,
                "wordCount": word_count,
                "path": rel_path,
                "sourceUrl": lwb_url,
                "topics": topics,
                "hasTranscript": True
            },
            "text": transcript_text
        }
    except Exception as e:
        print(f"   ⚠️  Skipping {filename}: {e}")
        return None

def cleanup_old_chunks(output_dir):
    """Remove existing chunk files before regenerating."""
    import glob
    pattern = os.path.join(output_dir, f"{SEARCH_CHUNK_PREFIX}*.json")
    old_chunks = glob.glob(pattern)
    if old_chunks:
        print(f"   Cleaning up {len(old_chunks)} old text chunk files...")
        for chunk_file in old_chunks:
            try:
                os.remove(chunk_file)
            except Exception as e:
                print(f"   Warning: Could not remove {chunk_file}: {e}")

def main():
    print("=" * 60)
    print("William Branham Sermon Archive - Data Generator")
    print("=" * 60)
    
    if not os.path.exists(DATA_DIR):
        print(f"❌ Data directory not found: {DATA_DIR}")
        print("   Run split_wmb_sermons.py first!")
        return
    
    ensure_dir(OUTPUT_DIR)
    cleanup_old_chunks(OUTPUT_DIR)
    
    all_meta = []
    current_chunk = []
    current_chunk_size = 0
    chunk_index = 0
    
    # Get all sermon files
    sermon_files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.txt')])
    print(f"📖 Processing {len(sermon_files)} sermon files...")
    
    for i, filename in enumerate(sermon_files):
        filepath = os.path.join(DATA_DIR, filename)
        data = parse_sermon(filepath, filename)
        
        if data:
            all_meta.append(data['meta'])
            
            text_entry = {"id": data['meta']['id'], "text": data['text']}
            text_size = len(data['text'].encode('utf-8'))
            current_chunk.append(text_entry)
            current_chunk_size += text_size
            
            if current_chunk_size >= CHUNK_SIZE_LIMIT:
                chunk_path = os.path.join(OUTPUT_DIR, f"{SEARCH_CHUNK_PREFIX}{chunk_index}.json")
                with open(chunk_path, 'w', encoding='utf-8') as f:
                    json.dump(current_chunk, f)
                print(f"   ✓ Wrote chunk {chunk_index} ({len(current_chunk)} sermons, {current_chunk_size / 1024 / 1024:.1f} MB)")
                current_chunk = []
                current_chunk_size = 0
                chunk_index += 1
        
        if (i + 1) % 200 == 0:
            print(f"   Processed {i + 1}/{len(sermon_files)} sermons...")
    
    # Write final chunk
    if current_chunk:
        chunk_path = os.path.join(OUTPUT_DIR, f"{SEARCH_CHUNK_PREFIX}{chunk_index}.json")
        with open(chunk_path, 'w', encoding='utf-8') as f:
            json.dump(current_chunk, f)
        print(f"   ✓ Wrote chunk {chunk_index} ({len(current_chunk)} sermons, {current_chunk_size / 1024 / 1024:.1f} MB)")
        chunk_index += 1
    
    # Calculate statistics
    total_words = sum(m['wordCount'] for m in all_meta)
    total_duration = sum(m.get('duration', 0) for m in all_meta)
    years = sorted(set(m['year'] for m in all_meta if m['year'] != 'Unknown'))
    venues = sorted(set(m.get('venue', 'Unknown') for m in all_meta))
    
    # Topic statistics
    topic_counts = {}
    for m in all_meta:
        for topic in m.get('topics', []):
            topic_counts[topic] = topic_counts.get(topic, 0) + 1
    
    # Build metadata
    master_data = {
        "generated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "totalChunks": chunk_index,
        "totalSermons": len(all_meta),
        "totalWords": total_words,
        "totalDurationMinutes": total_duration,
        "yearRange": f"{years[0]}-{years[-1]}" if years else "Unknown",
        "venues": venues,
        "topicCounts": dict(sorted(topic_counts.items(), key=lambda x: -x[1])),
        "sermons": sorted(all_meta, key=lambda x: x['timestamp'], reverse=True)
    }
    
    meta_path = os.path.join(OUTPUT_DIR, META_FILE)
    with open(meta_path, 'w', encoding='utf-8') as f:
        json.dump(master_data, f)
    
    print("\n" + "=" * 60)
    print("📊 GENERATION COMPLETE")
    print("=" * 60)
    print(f"   Total Sermons: {len(all_meta)}")
    print(f"   Total Words: {total_words:,}")
    print(f"   Total Duration: {total_duration:,} minutes ({total_duration // 60:,} hours)")
    print(f"   Year Range: {years[0] if years else '?'} - {years[-1] if years else '?'}")
    print(f"   Text Chunks: {chunk_index}")
    print(f"   Unique Venues: {len(venues)}")
    print(f"\n   Top Topics:")
    for topic, count in sorted(topic_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"      {topic}: {count} sermons")
    print(f"\n   Output: {OUTPUT_DIR}/")
    print("=" * 60)
    
    # Copy to docs/ for GitHub Pages
    import shutil
    import subprocess
    
    DOCS_DIR = "docs"
    print(f"\n   Syncing to {DOCS_DIR}/ for GitHub Pages...")
    
    def robust_rmtree(path):
        if not os.path.exists(path):
            return
        try:
            shutil.rmtree(path)
        except OSError:
            subprocess.run(['rm', '-rf', path], check=False)
    
    # Sync wmb_api folder
    docs_api = os.path.join(DOCS_DIR, OUTPUT_DIR)
    robust_rmtree(docs_api)
    shutil.copytree(OUTPUT_DIR, docs_api)
    print(f"   ✅ Copied {OUTPUT_DIR}/ → {docs_api}/")
    
    # Sync WMB data folder
    docs_data = os.path.join(DOCS_DIR, DATA_DIR)
    robust_rmtree(docs_data)
    shutil.copytree(DATA_DIR, docs_data)
    print(f"   ✅ Copied {DATA_DIR}/ → {docs_data}/")
    
    print("\n✅ Ready for BranhamApp.jsx to use!")

if __name__ == "__main__":
    main()
