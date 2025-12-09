import os
import json
import re
import datetime
import zipfile

# --- CONFIGURATION ---
DATA_DIR = "data"
OUTPUT_DIR = "site_api"
META_FILE = "metadata.json"
SEARCH_CHUNK_PREFIX = "text_chunk_"
CHUNK_SIZE_LIMIT = 5 * 1024 * 1024  # 5 MB
ZIP_FILENAME = "all_sermons_archive.zip"
MENTION_REGEX = r"(?:brother\s+william|william|brother)\s+br[aeiou]n[dh]*[aeiou]m"

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def parse_sermon(filepath, church, filename):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Regex Metadata Extraction
        # Now extracting Type and Language
        date_match = re.search(r"Date:\s+(.+)", content)
        title_match = re.search(r"Title:\s+(.+)", content)
        speaker_match = re.search(r"Speaker:\s+(.+)", content)
        type_match = re.search(r"Type:\s+(.+)", content)
        lang_match = re.search(r"Language:\s*(.+)", content)
        
        date_str = date_match.group(1).strip() if date_match else "Unknown Date"
        title = title_match.group(1).strip() if title_match else "Unknown Title"
        speaker = speaker_match.group(1).strip() if speaker_match else "Unknown Speaker"
        video_type = type_match.group(1).strip() if type_match else "Full Sermon"
        language = lang_match.group(1).strip() if lang_match else "English"
        
        try:
            ts = datetime.datetime.strptime(date_str, "%Y-%m-%d").timestamp() * 1000
            year = date_str[:4]
            month = date_str[5:7]
        except:
            ts = 0
            year = "Unknown"
            month = "Unknown"

        mentions = len(re.findall(MENTION_REGEX, content, re.IGNORECASE))
        word_count = len(content.split())
        
        # Web-safe relative path
        rel_path = f"data/{church}/{filename}".replace(" ", "%20")

        return {
            "meta": {
                "id": filename,
                "church": church,
                "date": date_str,
                "year": year,
                "month": month,
                "timestamp": ts,
                "title": title,
                "speaker": speaker,
                "type": video_type,
                "language": language,
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
    print("--- 🚀 GENERATING SITE DATA v2 ---")
    ensure_dir(OUTPUT_DIR)
    
    all_meta = []
    current_chunk = []
    current_chunk_size = 0
    chunk_index = 0
    
    with zipfile.ZipFile(ZIP_FILENAME, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for church_folder in os.listdir(DATA_DIR):
            church_path = os.path.join(DATA_DIR, church_folder)
            if os.path.isdir(church_path):
                # Handle underscores in folder names for display
                church_display = church_folder.replace("_", " ")
                print(f"   Processing: {church_display}...")
                
                for filename in os.listdir(church_path):
                    if filename.endswith(".txt"):
                        file_full_path = os.path.join(church_path, filename)
                        zipf.write(file_full_path, arcname=f"{church_folder}/{filename}")
                        
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

    print("\n✅ GENERATION COMPLETE")

if __name__ == "__main__":
    main()