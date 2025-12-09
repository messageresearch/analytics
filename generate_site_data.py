import os
import json
import re
import datetime
import zipfile
import shutil

# --- CONFIGURATION ---
DATA_DIR = "data"
OUTPUT_DIR = "site_api"  # We will put all JSONs here to keep root clean
META_FILE = "metadata.json"
SEARCH_CHUNK_PREFIX = "text_chunk_"
CHUNK_SIZE_LIMIT = 5 * 1024 * 1024  # 5 MB per search chunk
ZIP_FILENAME = "all_sermons_archive.zip"
MENTION_REGEX = r"(?:brother\s+william|william|brother)\s+br[aeiou]n[dh]*[aeiou]m"

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def parse_sermon(filepath, church, filename):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Regex metadata extraction
        date_match = re.search(r"Date:\s+(.+)", content)
        title_match = re.search(r"Title:\s+(.+)", content)
        speaker_match = re.search(r"Speaker:\s+(.+)", content)
        
        date_str = date_match.group(1).strip() if date_match else "Unknown Date"
        title = title_match.group(1).strip() if title_match else "Unknown Title"
        speaker = speaker_match.group(1).strip() if speaker_match else "Unknown Speaker"
        
        # Timestamp for sorting
        try:
            ts = datetime.datetime.strptime(date_str, "%Y-%m-%d").timestamp() * 1000
        except:
            ts = 0

        # Metrics
        mentions = len(re.findall(MENTION_REGEX, content, re.IGNORECASE))
        word_count = len(content.split())
        
        # Relative path for fetching raw text
        # Force forward slashes for Web URLs
        rel_path = os.path.join(DATA_DIR, church.replace(" ", "_"), filename).replace("\\", "/")

        return {
            "meta": {
                "id": filename,
                "church": church,
                "date": date_str,
                "timestamp": ts,
                "title": title,
                "speaker": speaker,
                "mentionCount": mentions,
                "wordCount": word_count,
                "path": rel_path
            },
            "text": content
        }
    except Exception as e:
        print(f"Skipping {filename}: {e}")
        return None

def main():
    print("--- 🚀 STARTING SITE GENERATION ---")
    ensure_dir(OUTPUT_DIR)
    
    all_meta = []
    current_chunk = []
    current_chunk_size = 0
    chunk_index = 0
    
    # Initialize Zip File
    zip_path = ZIP_FILENAME
    print(f"   📦 Creating Zip Archive: {zip_path}...")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        
        # Walk Data Directory
        for church_folder in os.listdir(DATA_DIR):
            church_path = os.path.join(DATA_DIR, church_folder)
            if os.path.isdir(church_path):
                church_clean = church_folder.replace("_", " ")
                print(f"   Processing: {church_clean}...")
                
                for filename in os.listdir(church_path):
                    if filename.endswith(".txt"):
                        file_full_path = os.path.join(church_path, filename)
                        
                        # Add to Zip
                        zipf.write(file_full_path, arcname=f"{church_clean}/{filename}")
                        
                        # Process Data
                        data = parse_sermon(file_full_path, church_clean, filename)
                        if data:
                            # Add to Metadata List (Lightweight)
                            all_meta.append(data['meta'])
                            
                            # Add to Search Chunk (Heavy)
                            text_entry = {"id": data['meta']['id'], "text": data['text']}
                            text_size = len(data['text'].encode('utf-8'))
                            
                            current_chunk.append(text_entry)
                            current_chunk_size += text_size
                            
                            # If chunk exceeds limit, save it and start new one
                            if current_chunk_size >= CHUNK_SIZE_LIMIT:
                                chunk_name = f"{SEARCH_CHUNK_PREFIX}{chunk_index}.json"
                                with open(os.path.join(OUTPUT_DIR, chunk_name), 'w', encoding='utf-8') as f:
                                    json.dump(current_chunk, f)
                                print(f"      🔹 Saved Search Chunk #{chunk_index} ({len(current_chunk)} sermons)")
                                current_chunk = []
                                current_chunk_size = 0
                                chunk_index += 1

    # Save final partial chunk
    if current_chunk:
        chunk_name = f"{SEARCH_CHUNK_PREFIX}{chunk_index}.json"
        with open(os.path.join(OUTPUT_DIR, chunk_name), 'w', encoding='utf-8') as f:
            json.dump(current_chunk, f)
        print(f"      🔹 Saved Search Chunk #{chunk_index} ({len(current_chunk)} sermons)")
        chunk_index += 1

    # Save Master Metadata File
    # We include 'totalChunks' so the React app knows how many files to search
    master_data = {
        "generated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "totalChunks": chunk_index,
        "sermons": sorted(all_meta, key=lambda x: x['timestamp'], reverse=True)
    }
    
    with open(os.path.join(OUTPUT_DIR, META_FILE), 'w', encoding='utf-8') as f:
        json.dump(master_data, f)

    print("\n✅ GENERATION COMPLETE")
    print(f"   Total Sermons: {len(all_meta)}")
    print(f"   Search Chunks: {chunk_index}")
    print(f"   Archive Size:  {os.path.getsize(zip_path) / (1024*1024):.2f} MB")

if __name__ == "__main__":
    main()