
import os
import re
import json
import datetime
import argparse
import time
import random
import requests
import xml.etree.ElementTree as ET
import scrapetube
import spacy
import platform
import subprocess
import csv
import shutil
import ShalomTabernacleSermonScraperv2 as st_scraper
from pytubefix import YouTube
from pytubefix.cli import on_progress

# --- CONFIGURATION ---
CONFIG_FILES = ["channels.json", "config.json"]
SPEAKERS_FILE = "speakers.json"
DATA_DIR = "data"
SHALOM_CSV_NAME = "shalom_tabernacle_sermons.csv"

# AGE THRESHOLD: Videos uploaded within this many days are considered "Recent"
RECENT_VIDEO_THRESHOLD_DAYS = 180 

# COOL-DOWN: How long to wait before re-checking an OLD video (> 6 months old)
OLD_VIDEO_CHECK_INTERVAL_DAYS = 180

# Load NLP Model
try:
    nlp = spacy.load("en_core_web_sm")
except:
    print("⚠️ Spacy model not found. Run: python3 -m spacy download en_core_web_sm")
    nlp = None

# Terms that indicate a candidate is likely a Title/Topic, NOT a person
INVALID_NAME_TERMS = {
    # religious terms
    "jesus", "christ", "god", "lord", "bible", "scripture", "prophet",
    "tucson", "tabernacle", "arizona", "church", "service", "shekinah",
    "prayer", "worship", "testimony", "testimonies", "communion", "baptism",
    "evening", "morning", "wednesday", "sunday", "dedication", "meeting", "study",
    "sermon", "message", "part", "chapter", "verse", "volume", "thunders", "reviewing",
    "live", "stream", "update", "announcement", "q&a",
    "discussion", "teaching", "preaching", "song", "music", "choir", "harmony",
    "guest", "minister", "ministry", "revival", "conference",
    "report", "mission", "position", "clip", "wedding", "book", "items",
    "carriers", "vessel", "partnership", "seed", "garden", "situations",
    "control", "life", "power", "glory", "faith", "love", "hope", "fear",
    "video", "series", "restoration", "process", "year", "month",
    "day", "hour", "answer", "me", "you", "us", "them", "it", "words",
    "program", "skit", "singing", "drama", "play", "memorial", "celebration",
    "vbs", "cancel", "culture", "night", "altar", "call", "civil", "war", "project",
    # song lyrics / sermon title words
    "here", "room", "enemies", "scattered", "masterpiece", "holding", "another", "fire",
    "grudges", "holy", "convocations", "great", "thou", "art", "deer", "because",
    "choose", "die", "need", "thee", "every", "speak", "stand", "courage", "reign",
    "amazed", "thank", "want", "more", "worthy", "vida", "near", "lost",
    "fort", "collins", "friend", "alpha", "elijah", "parallel", "carried", "table",
    "crossed", "over", "token", "applied", "display", "real", "id", "face",
    "ready", "translation", "cycle", "death", "approval", "works", "kingdom", "sessions",
    "things", "that", "were", "was", "this", "those", "these", "are", "be", "if",
    "have", "had", "do", "did", "say", "said", "see", "saw", "go", "went", "did",
    "come", "came", "give", "gave", "make", "made", "know", "still",
    "think", "thought", "get", "got", "find", "found", "way", "ways", "one", "him",
    "two", "three", "first", "second", "third", "new", "old", "good", "bad", "gardens",
    "high", "low", "big", "small", "own", "other", "some", "all", "any", "goodness",
    "our", "your", "his", "her", "their", "its", "my", "mine", "yours", "greater",
    "from", "with", "in", "on", "at", "by", "to", "for", "of", "up", "down", "through",
    "out", "about", "into", "over", "after", "before", "under", "between",
    "so", "am", "is", "he", "she", "they", "we", "not", "but", "or", "nor", "can", "yes", "shall",
    "servants", "bride", "law", "trip", "demand", "judgment", "seal",
    "already", "praised", "box", "alabaster", "mountain", "lead", "redeemed",
    # language / foreign terms
    "servicio", "reunion", "en", "frances", "espanol", "domingo",
    "miercoles", "ibikorwa", "ikirundi", "vy", "vas"
}

SONG_TITLES = {
    "When The Redeemed", "As The Deer", "Come Jesus Come", "Pray On", "Walking With God",
    "Take Courage", "He Did It", "No Civil War", "Last Words", "Give It To God",
    "Speak To The Mountain", "Yes He Can", "His Name Is Jesus, Shekinah Tabernacle",
    "Never Lost", "Rapturing Faith Believers Fellowship, Willing Vessel", "When Love Project",
    "Spirit Lead Me", "Shout To The Lord", "Because Of Jesus", "Graves Into Gardens",
    "Shall Not Want", "Through The Fire", "Another In The Fire", "Goodness Of God",
    "Look Up", "Trust In God", "Even If", "Carried To The Table", "Another In The Fire",
    "As The Deer", "Because Of Jesus", "Carried To The Table", "Even If", "Face To Face",
    "Fear Is A Liar", "Give It To God", "Goodness Of God", "Graves Into Gardens",
    "Greater Than Great", "He Did It", "He Still Speaks", "Last Words", "Look Up",
    "Near The Cross", "Never A Time", "Never Lost", "No Civil War", "No Fear",
    "Pour Oil", "Pray On", "Shall Not Want", "Shout To The Lord", "Speak To The Mountain",
    "Spirit Lead Me", "Take Courage", "Through The Fire", "Trust In God",
    "Walking With God", "We Have Found Him", "When Love Project", "When The Redeemed",
    "Yes He Can", "You Reign", "Algo Está Cayendo Aquí", "Until Then", "When The"
}

CATEGORY_TITLES = {
    "Testimonies", "Prayer Meeting", "Worship Night", "Prayer Bible Study",
    "New Years Day Items", "Picture Slideshow"
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64 x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def prevent_sleep():
    if platform.system() == 'Darwin':
        try:
            subprocess.Popen(['caffeinate', '-i', '-w', str(os.getpid())])
        except: pass

def get_random_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://www.youtube.com/",
    }

def load_json_file(filepath):
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return set(json.load(f))
        except: return set()
    return set()

def save_json_file(filepath, data):
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(sorted(list(data)), f, indent=2)
    except Exception as e: pass

def load_config():
    for config_file in CONFIG_FILES:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f: return json.load(f)
    return {}

# --- TEXT CLEANING & NORMALIZATION ---
def sanitize_filename(text):
    return re.sub(r'[\\/*?:"<>|]', "", text).strip()

def split_multiple_speakers(text):
    return [p.strip() for p in re.split(r'\s+&\s+|\s+and\s+|\s*/\s*|,\s*', text) if p.strip()]

def normalize_speaker(speaker):
    if not speaker or speaker == "Unknown Speaker": return speaker
    s_lower = speaker.lower()
    
    # Specific fix maps
    if "prophet" in s_lower and "branham" in s_lower: return "William M. Branham"
    if "william" in s_lower and "branham" in s_lower: return "William M. Branham"
    if "isaiah" in s_lower and "brooks" in s_lower: return "Isiah Brooks"
    if "caleb" in s_lower and "perez" in s_lower: return "Caleb Perez"
    if "daniel" in s_lower and "evans" in s_lower: return "Daniel Evans"
    if "andrew glover" in s_lower and "twt camp" in s_lower: return "Andrew Glover"
    if "andrew spencer" in s_lower and "july" in s_lower: return "Andrew Spencer"
    if "pr" in s_lower and "busobozi" in s_lower: return "Busobozi Talemwa"
    if "joel pruitt" in s_lower and "youth" in s_lower: return "Joel Pruitt"

    # Choir Fixes
    if "choir" in s_lower:
        if "evening light" in s_lower: return "Evening Light Choir"
        if "bethel" in s_lower: return "Bethel Tabernacle Choir"
        return "Church Choir"

    return speaker.strip()

def clean_name(name):
    # Remove Titles
    name = re.sub(r'^(By|Pr\.?|Br\.?|Bro\.?|Brother|Brothers|Bros\.?|Sister|Sis\.?|Hna\.?|Hno\.?|Pastor|Bishop|Rev\.?|Evangelist|Guest Minister|Song Leader|Elder|Founding)\s+', '', name, flags=re.IGNORECASE)
    # Remove Trailing Punctuation
    name = name.strip(" .,:;-|")
    # Remove trailing junk words
    words = name.split()
    while words and words[-1].lower() in INVALID_NAME_TERMS:
        words.pop()
    return " ".join(words)

# --- AUTHORITATIVE SPEAKER CHECK ---
def get_canonical_speaker(speaker, speakers_set, speakers_file):
    """
    Checks if the given speaker (first + last name) exists in speakers_set (from speakers.json).
    If yes, returns the canonical name. If not, adds it to speakers.json and returns the new name.
    """
    if not speaker or speaker == "Unknown Speaker":
        return "Unknown Speaker"
    # Try to match by first + last name (case-insensitive, ignoring extra spaces)
    def normalize_name(n):
        return " ".join(n.strip().split()).lower()
    speaker_norm = normalize_name(speaker)
    for s in speakers_set:
        if normalize_name(s) == speaker_norm:
            return s  # Return canonical from list
    # If not found, add to speakers.json
    speakers_set.add(speaker.strip())
    save_json_file(speakers_file, speakers_set)
    print(f"[NEW SPEAKER ADDED] '{speaker.strip()}' added to {speakers_file}")
    return speaker.strip()

def is_valid_person_name(text):
    text = text.strip()
    if not text: return False
    # Add specific exceptions for valid names that might otherwise fail
    if text.lower() in ["church choir", "bloteh won", "chris take", "tim cross"]:
        return True
    t_lower = text.lower()
    
    # Reject obivous junk
    if t_lower.startswith("the ") or t_lower.startswith("a ") or t_lower.startswith("an "): return False
    if t_lower.startswith("i ") or t_lower.startswith("my ") or t_lower.startswith("if "): return False
    if "hymn" in t_lower or "service" in t_lower: return False
    
    text_words = t_lower.split()
    for word in text_words:
        w_clean = word.strip(".,:;-")
        if w_clean in INVALID_NAME_TERMS:
            return False
            
    words = text.split()
    if not (2 <= len(words) <= 5): return False
    
    # Capitalization Check
    allowed_lowercase = {'de', 'la', 'van', 'der', 'st', 'mc', 'mac', 'del', 'dos', 'da', 'von'}
    for w in words:
        clean_w = w.replace('.', '')
        if not clean_w.isalpha(): return False
        if not w[0].isupper():
            if w.lower() not in allowed_lowercase: return False
            
    if nlp:
        doc = nlp(text)
        for ent in doc.ents:
            if ent.label_ in ["ORG", "DATE", "TIME", "GPE", "PRODUCT", "FAC"]: return False
    return True

# --- ADVANCED HEALING LOGIC ---
def analyze_content_for_song(filepath):
    # Read the file content
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"[analyze_content_for_song] Failed to read {filepath}: {e}")
        return False

    # Extract just the transcript body (skip header)
    body = content.split("========================================")[-1]

    music_tag_count = body.count('[Music]') + body.count('(Music)')
    text_length = len(body)

    # Heuristic 1: Short & Music Heavy
    if text_length < 3000 and music_tag_count >= 2:
        return True

    # Heuristic 2: Very Short (likely just lyrics)
    if text_length < 1000 and music_tag_count >= 1:
        return True
        
    return False

def smart_speaker_correction(current_speaker, title):
    """
    Attempts to fix "Bad Speaker" entries like:
    - "Aaron McGeary, Emotional Faith" (Name + Title)
    - "Already Won" (Title mistakenly used as Name)
    """
    # 1. Clean basic prefixes
    clean = clean_name(current_speaker)
    norm = normalize_speaker(clean)
    
    # 2. Check for "Name, Title" pattern. Be less aggressive.
    # If a comma is present, trust that it's separating speakers and just clean them.
    if "," in norm:
        parts = split_multiple_speakers(norm)
        cleaned_parts = [clean_name(p) for p in parts]
        return ", ".join(cleaned_parts)

    # 3. Check for "Title as Speaker" pattern
    # If the current speaker string is found identically in the Title, 
    # and the Title has OTHER words that look like a Name.
    if norm in title and not is_valid_person_name(norm):
        # Remove the "bad speaker" string from the title to see what's left
        remainder = title.replace(norm, "").strip(" -|:,&")
        # If remainder looks like a name, assume THAT is the real speaker
        possible_name = clean_name(remainder)
        if is_valid_person_name(possible_name):
            return normalize_speaker(possible_name)

    return norm

UNWANTED_SPEAKERS = set([
    # Duplicates and non-speaker entries to heal
    "Eduan Naude", "Eduan Naudé", "Forest Farmer The Fruit", "Forrest Farmer",
    "Financial Jubilee", "Finding Yourself", "Fitly Joined Together", "Five Comings",
    "Free Indeed Bro Joe Reynolds", "Freedom Released", "Fulfilling The Original"
])

def heal_archive(data_dir, force=False):
    print("\n" + "="*60)
    print("🚑 STARTING DEEP ARCHIVE HEALING & CLEANUP")
    print(f"heal_archive called with data_dir={data_dir}, force={force}")
    if force:
        print("   ⚠️ FORCE MODE: Re-processing all entries.")
    print("="*60)
    print("About to iterate church folders...")
    
    updated_files_count = 0
    cleaned_speakers = set()
    # Shadow master summary file for detected speakers
    SHADOW_MASTER_FILE = os.path.join(data_dir, "shadow_master_speakers.csv")
    shadow_rows = []
    shadow_header = ["speaker_name", "source", "detected_date", "notes"]
    
    # 1. Iterate over every Church Folder
    for church_folder in os.listdir(data_dir):
        church_path = os.path.join(data_dir, church_folder)
        if not os.path.isdir(church_path): continue
        
        summary_path = os.path.join(data_dir, f"{church_folder}_Summary.csv")
        if not os.path.exists(summary_path): continue
        
        print(f"   🏥 Healing: {church_folder.replace('_', ' ')}...")
        
        new_rows = []
        headers = []
        
        try:
            with open(summary_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames
                rows = list(reader)
        except: continue
        
        # Build a set of (date, title, speaker) from current summary for fast lookup
        summary_keys = set()
        for row in rows:
            key = (row.get('date', '').strip(), row.get('title', '').strip(), row.get('speaker', '').strip())
            summary_keys.add(key)

        # --- NEW LOGIC: Ensure every .txt transcript is represented in the summary CSV ---
        txt_files = [f for f in os.listdir(church_path) if f.endswith('.txt')]
        print(f"   📄 Found {len(txt_files)} .txt files in {church_folder}")
        for txt_file in txt_files:
            base = os.path.splitext(txt_file)[0]
            parts = base.split(' - ')
            if len(parts) >= 3:
                date, title, speaker = parts[0].strip(), parts[1].strip(), parts[2].strip()
            else:
                date, title, speaker = '', base, ''
            if (date, title, speaker) not in summary_keys:
                print(f"      ➕ Adding missing transcript: {txt_file}")
                filepath = os.path.join(church_path, txt_file)
                language = 'Unknown'
                video_type = 'Full Sermon'
                url = ''
                last_checked = datetime.datetime.now().strftime("%Y-%m-%d")
                status = 'Success'
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        content = f.read(2048)
                        match = re.search(r'(https://www\.youtube\.com/watch\?v=[\w-]+)', content)
                        if match:
                            url = match.group(1)
                except Exception:
                    pass
                new_row = {
                    "date": date,
                    "status": status,
                    "speaker": speaker,
                    "title": title,
                    "url": url,
                    "last_checked": last_checked,
                    "language": language,
                    "type": video_type
                }
                rows.append(new_row)
                summary_keys.add((date, title, speaker))
                updated_files_count += 1
            else:
                print(f"      ✔ Already in summary: {txt_file}")

        for row in rows:
            original_speaker = row.get('speaker', 'Unknown Speaker')
            # Collect detected speaker for shadow master file
            detected_date = datetime.datetime.now().strftime("%Y-%m-%d")
            shadow_rows.append({
                "speaker_name": original_speaker,
                "source": church_folder,
                "detected_date": detected_date,
                "notes": "Detected during healing"
            })
            original_title = row.get('title', '')
            original_date = row.get('date', '')
            original_type = row.get('type', 'Full Sermon')

            # --- HEALING: Reassign unwanted/invalid speakers ---
            if original_speaker.strip() in UNWANTED_SPEAKERS:
                print(f"      - Healing unwanted speaker: '{original_speaker}' -> 'Unknown Speaker'")
                original_speaker = 'Unknown Speaker'
                row['speaker'] = 'Unknown Speaker'
            

            # --- STEP 1: SMART SPEAKER & TYPE CORRECTION ---
            new_speaker = original_speaker
            new_type = original_type
            cleaned_original_speaker = original_speaker.strip()

            if cleaned_original_speaker in SONG_TITLES:
                new_speaker = "Unknown Speaker"
                new_type = "Song / Worship"
            elif cleaned_original_speaker in CATEGORY_TITLES:
                new_speaker = "Unknown Speaker"
                new_type = cleaned_original_speaker
            elif cleaned_original_speaker == "Harmony":
                new_speaker = "Unknown Speaker"
                new_type = "Harmony In The Desert"
            elif cleaned_original_speaker == "Christian Life Tabernacle":
                new_speaker = "Unknown Speaker"
            elif cleaned_original_speaker in UNWANTED_SPEAKERS:
                new_speaker = "Unknown Speaker"
            else:
                # Only run smart correction if it's not a special case
                new_speaker = smart_speaker_correction(original_speaker, original_title)

            # --- STEP 2: SONG DETECTION FROM CONTENT (can override above) ---
            # Construct filename to find transcript
            safe_title = sanitize_filename(original_title)
            safe_old_speaker = sanitize_filename(original_speaker)
            old_filename = f"{original_date} - {safe_title} - {safe_old_speaker}.txt"
            old_filepath = os.path.join(church_path, old_filename)
            
            is_song = False
            if os.path.exists(old_filepath):
                is_song = analyze_content_for_song(old_filepath)
                
            if is_song:
                new_type = "Song / Worship"
            elif "choir" in new_speaker.lower():
                new_type = "Choir"
                
            # --- STEP 3: APPLY UPDATES ---
            
            # If nothing changed, keep row as is
            if not force and new_speaker == original_speaker and new_type == original_type:
                new_rows.append(row)
                cleaned_speakers.add(new_speaker)
                continue
                
            # SOMETHING CHANGED -> UPDATE FILE & DB
            change_detected = False
            if new_speaker != original_speaker:
                print(f"      - Speaker Change: '{original_speaker}' -> '{new_speaker}'")
                change_detected = True
            if new_type != original_type:
                print(f"      - Type Change: '{original_type}' -> '{new_type}'")
                change_detected = True

            # 1. Rename File
            safe_new_speaker = sanitize_filename(new_speaker)
            new_filename = f"{original_date} - {safe_title} - {safe_new_speaker}.txt"
            new_filepath = os.path.join(church_path, new_filename)
            
            if os.path.exists(old_filepath):
                # Update Header Content
                try:
                    with open(old_filepath, 'r', encoding='utf-8') as tf:
                        content = tf.read()
                    
                    # Regex Replace Header Info
                    content = re.sub(r'Speaker:.*', f'Speaker: {new_speaker}', content)
                    content = re.sub(r'Type:.*', f'Type:    {new_type}', content)
                    content = re.sub(r'START OF FILE:.*', f'START OF FILE: {new_filename}', content)
                    
                    # Rename and Write
                    with open(new_filepath, 'w', encoding='utf-8') as tf:
                        tf.write(content)
                        
                    if new_filename != old_filename:
                        os.remove(old_filepath)
                        
                except Exception as e:
                    print(f"      ❌ Failed to rewrite file {old_filename}: {e}")
            
            # 2. Update CSV Row
            # Always check against authoritative speakers.json
            canonical_speaker = get_canonical_speaker(new_speaker, cleaned_speakers, SPEAKERS_FILE)
            row['speaker'] = canonical_speaker
            row['type'] = new_type
            new_rows.append(row)
            cleaned_speakers.add(canonical_speaker)
            if change_detected or force:
                updated_files_count += 1
            
        # Write back updated Summary CSV
        try:
            with open(summary_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(new_rows)
        except: pass

    # --- STEP 4: UPDATE SPEAKERS.JSON ---
    # Only keep valid names found in the healed archive, then apply advanced cleaning/deduplication
    def clean_and_extract_speakers(raw_list):
        cleaned_names = []
        stop_words = [
            " The ", " A ", " An ", " In ", " On ", " Of ", " And ", " Vs ",
            " At ", " For ", " With ", " To ", " From ", " By ", " Pt ", " Part "
        ]
        blocklist = [
            "Family Camp", "Wed Eve", "Wednesday Eve", "Sun Morn", "Sunday Morning",
            "Unknown Speaker", "Guest Speaker", "Various Speakers", "Song Service",
            "Communion", "Testimony", "Prayer", "Worship", "Tape"
        ]
        for entry in raw_list:
            temp_name = entry.strip()
            for word in stop_words:
                if word in temp_name:
                    index = temp_name.find(word)
                    temp_name = temp_name[:index]
                    break
            temp_name = re.sub(r'\\d+', '', temp_name).strip()
            temp_name = temp_name.rstrip(":-_")
            word_count = len(temp_name.split())
            if word_count < 2 or word_count > 3:
                continue
            if any(bad_term.lower() in temp_name.lower() for bad_term in blocklist):
                continue
            cleaned_names.append(temp_name)
        return list(set(cleaned_names))

    def fuzzy_deduplicate_speakers(candidates):
        from difflib import SequenceMatcher
        final_speakers = set()
        sorted_candidates = sorted(candidates, key=len, reverse=True)
        for name in sorted_candidates:
            is_duplicate = False
            for existing in final_speakers:
                ratio = SequenceMatcher(None, name, existing).ratio()
                if (name in existing or existing in name) and ratio > 0.6:
                    is_duplicate = True
                    break
            if not is_duplicate:
                final_speakers.add(name)
        return sorted(list(final_speakers))

    # Step 1: Only keep valid names found in the healed archive
    initial_candidates = [s for s in cleaned_speakers if is_valid_person_name(s)]
    # Step 2: Clean and extract
    cleaned_candidates = clean_and_extract_speakers(initial_candidates)
    # Step 3: Fuzzy deduplication
    final_speakers = fuzzy_deduplicate_speakers(cleaned_candidates)
    save_json_file(SPEAKERS_FILE, final_speakers)
    # Write shadow master summary file
    try:
        with open(SHADOW_MASTER_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=shadow_header)
            writer.writeheader()
            writer.writerows(shadow_rows)
        print(f"🗂️ Shadow master summary updated: {SHADOW_MASTER_FILE}")
    except Exception as e:
        print(f"❌ Failed to update shadow master summary: {e}")
    print(f"✅ Healing Complete. Corrected {updated_files_count} files/entries.")
    print(f"✨ Global Speaker List Optimized: {len(final_speakers)} unique speakers.")

# --- MAIN LOGIC (UNCHANGED EXCEPT MENU) ---
# ... (Previous helper functions identify_speaker_dynamic, process_channel, etc. remain here) ...
# For brevity, I am including the critical `process_channel` and `main` updates below.

def identify_speaker_dynamic(title, description, known_speakers):
    found_speakers = set()
    search_text = f"{title}\n{description}"
    for name in known_speakers:
        if name in search_text: found_speakers.add(name)

    # Patterns (same as before)
    pipe_date_pattern = r"(?:\b|^)(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4}\s*\|\s*(?:Pastor|Bro\.?|Brother|Bishop|Elder|Rev\.?)\s+([A-Z][a-zA-Z\.]+(?:\s+[A-Z][a-zA-Z\.]+)+)(?:\s+[-–—|])"
    match = re.search(pipe_date_pattern, title)
    if match:
        candidate = clean_name(match.group(1))
        if is_valid_person_name(candidate): return normalize_speaker(candidate), True

    date_name_pattern = r"(?:\b|^)\d{6}\s*[-–—:|]\s*([A-Za-z\.\s]+)(?:\s*[-–—:|]|\s+on\s+)"
    match = re.search(date_name_pattern, title)
    if match:
        candidate = clean_name(match.group(1))
        if is_valid_person_name(candidate): return normalize_speaker(candidate), True

    pattern = r"(?:Bro\.?|Brother|Brothers|Bros\.?|Pastor|Bishop|Rev\.?|Evangelist|Guest Minister|Elder|Hno\.?|Hna\.?)\s+([A-Z][a-zA-Z\.]+(?:[\s]+[A-Z][a-zA-Z\.]+)*)"
    matches = re.findall(pattern, title)
    for match in matches:
        clean = clean_name(match)
        if is_valid_person_name(clean): return normalize_speaker(clean), True

    on_date_pattern = r"(?:Bro\.?|Brother|Pastor|Bishop)\s+([A-Z][a-zA-Z\.]+(?:\s+[A-Z][a-zA-Z\.]+)+)\s+on\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
    match = re.search(on_date_pattern, title)
    if match:
        clean = clean_name(match.group(1))
        if is_valid_person_name(clean): return normalize_speaker(clean), True

    if not found_speakers:
        separators = [r'\s[-–—]\s', r'\s[:|]\s', r'\sby\s']
        for sep in separators:
            parts = re.split(sep, title)
            if len(parts) > 1:
                candidate = clean_name(parts[0].strip())
                if is_valid_person_name(candidate): found_speakers.add(candidate)
                candidate_end = clean_name(parts[-1].strip())
                if is_valid_person_name(candidate_end): found_speakers.add(candidate_end)

    if found_speakers:
        final_list = consolidate_names(found_speakers)
        normalized_list = [normalize_speaker(s) for s in final_list]
        normalized_list = sorted(list(set(normalized_list)))
        if normalized_list: return ", ".join(normalized_list), False

    return "Unknown Speaker", False

def determine_video_type(title, speaker):
    title_lower = title.lower()
    if speaker == "William M. Branham": return "Tape Service"
    if "memorial" in title_lower or "celebrating the life" in title_lower or "funeral" in title_lower: return "Memorial Service"
    if "wedding" in title_lower: return "Wedding"
    if "youth camp" in title_lower: return "Youth Camp"
    if "christmas" in title_lower or "nativity" in title_lower: return "Christmas Program"
    if "clip" in title_lower: return "Sermon Clip"
    if ("worship" in title_lower or "song" in title_lower) and "song of solomon" not in title_lower:
        if speaker == "Unknown Speaker": return "Worship Service"
    return "Full Sermon"

def determine_language(title, yt_obj):
    title_lower = title.lower()
    if "espanol" in title_lower or "español" in title_lower or "spanish" in title_lower or "servicio en" in title_lower: return "Spanish"
    if "en frances" in title_lower or "francais" in title_lower or "français" in title_lower or "french" in title_lower: return "French"
    if "ikirundi" in title_lower or "kirundi" in title_lower or "ibikorwa" in title_lower: return "Kirundi"
    if "portugues" in title_lower or "português" in title_lower or "portuguese" in title_lower: return "Portuguese"
    if "swahili" in title_lower or "kiswahili" in title_lower: return "Swahili"
    if not yt_obj: return "Unknown"
    try:
        captions = yt_obj.captions
        if 'en' in captions or 'a.en' in captions or 'en-US' in captions: return "English"
        if 'es' in captions or 'a.es' in captions: return "Spanish"
        if 'fr' in captions: return "French"
        if 'pt' in captions: return "Portuguese"
        if len(captions) > 0: return str(list(captions.keys())[0])
    except: pass
    return "Unknown"

def validate_year(date_str):
    try:
        dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        if dt.year < 2005: return None 
        return date_str
    except: return None

def extract_date_from_text(text):
    match = re.search(r'(\d{4})[-./](\d{2})[-./](\d{2})', text)
    if match: return validate_year(f"{match.group(1)}-{match.group(2)}-{match.group(3)}")
    match = re.search(r'(\d{2})[-./](\d{2})[-./](\d{4})', text)
    if match: return validate_year(f"{match.group(3)}-{match.group(1)}-{match.group(2)}")
    match = re.search(r'\b(2[0-9])(\d{2})(\d{2})\b', text)
    match = re.search(r'([A-Z][a-z]+)\s+(\d{1,2}),?\s+(\d{4})', text)
    if match:
        try:
            dt = datetime.datetime.strptime(f"{match.group(1)} {match.group(2)} {match.group(3)}", "%B %d %Y")
            return validate_year(dt.strftime("%Y-%m-%d"))
        except: pass
    return None

def determine_sermon_date(title, description, yt_obj):
    date = extract_date_from_text(title)
    if date: return date
    date = extract_date_from_text(description)
    if date: return date
    if yt_obj:
        try: return yt_obj.publish_date.strftime("%Y-%m-%d")
        except: pass
    return "Unknown Date"

def format_sermon_entry(video_id, title, date_str, transcript_text, church_name, speaker, language, video_type):
    return (
        f"################################################################################\n"
        f"START OF FILE: {date_str} - {title} - {speaker} - Clean.txt\n"
        f"################################################################################\n\n"
        f"SERMON DETAILS\n"
        f"========================================\n"
        f"Date:    {date_str}\n"
        f"Title:   {title}\n"
        f"Speaker: {speaker}\n"
        f"Church:  {church_name}\n"
        f"Type:    {video_type}\n"
        f"Language:{language}\n"
        f"URL:     https://www.youtube.com/watch?v={video_id}\n"
        f"========================================\n\n"
        f"{transcript_text}\n"
    )
def xml_to_text(xml_content):
    try:
        root = ET.fromstring(xml_content)
        clean_lines = []
        for child in root:
            if child.tag == 'text':
                text = child.text or ""
                text = text.replace('&nbsp;', ' ').replace('&#39;', "'").replace('&quot;', '"').replace('&amp;', '&')
                text = " ".join(text.split())
                if text: clean_lines.append(text)
        return " ".join(clean_lines)
    except: return None

def fetch_captions_with_client(video_id, client_type):
    url = f"https://www.youtube.com/watch?v={video_id}"
    yt = YouTube(url, client=client_type, use_oauth=False, allow_oauth_cache=False)
    try: _ = yt.title 
    except: return None, None
    caption_track = None
    search_order = ['en', 'a.en', 'en-US', 'es', 'a.es', 'fr', 'pt']
    for code in search_order:
        if code in yt.captions:
            caption_track = yt.captions[code]; break
    if not caption_track:
        for code in yt.captions:
            caption_track = yt.captions[code]; break
    return caption_track, yt

def get_transcript_data(video_id):
    caption_track = None
    yt_obj = None
    try: caption_track, yt_obj = fetch_captions_with_client(video_id, 'WEB')
    except: pass
    if not caption_track:
        try: caption_track, yt_obj = fetch_captions_with_client(video_id, 'ANDROID')
        except: pass
    if not yt_obj: return None, "", None
    description = ""
    try: description = yt_obj.description or ""
    except: pass
    if not caption_track: return None, description, yt_obj
    try:
        response = requests.get(caption_track.url, headers=get_random_headers())
        if response.status_code == 200:
            clean_text = xml_to_text(response.text)
            return clean_text, description, yt_obj
        else: return None, description, yt_obj
    except: return None, description, yt_obj

def load_shalom_csv(filepath):
    videos = []
    if os.path.exists(filepath):
        print(f"   📂 Loading Shalom CSV Database: {filepath}")
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get("YouTube URL", "").strip()
                    if "youtube.com" in url or "youtu.be" in url:
                        if "v=" in url: vid = url.split("v=")[1].split("&")[0]
                        else: vid = url.split("/")[-1].split("?")[0]
                        video_obj = {
                            'videoId': vid,
                            'title': {'runs': [{'text': row.get("Sermon Title", "Unknown Title")}]},
                            'manual_date': row.get("Sermon Date"),
                            'manual_speaker': row.get("Speaker"),
                            'webpage_url': url,
                            'website_url': row.get("Website URL", "")
                        }
                        videos.append(video_obj)
        except Exception as e: print(f"   ⚠️ Error reading Shalom CSV: {e}")
    else:
        print(f"   ⚠️ Shalom CSV not found at {filepath}")
    return videos

def update_shalom_csv(filepath, all_videos):
    csv_rows = []
    for v in all_videos:
        title_text = "Unknown Title"
        if isinstance(v.get('title'), dict):
             runs = v['title'].get('runs', [])
             if runs: title_text = runs[0].get('text', 'Unknown Title')
        else:
             title_text = v.get('title', 'Unknown Title')
        yt_url = v.get('webpage_url')
        if not yt_url: yt_url = f"https://www.youtube.com/watch?v={v['videoId']}"
            
        csv_rows.append({
            'Sermon Date': v.get('manual_date', '0000-00-00'),
            'Sermon Title': title_text,
            'Speaker': v.get('manual_speaker', 'Unknown Speaker'),
            'YouTube URL': yt_url,
            'Website URL': v.get('website_url', '')
        })
    csv_rows.sort(key=lambda x: x['Sermon Date'], reverse=True)
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['Sermon Date', 'Sermon Title', 'Speaker', 'YouTube URL', 'Website URL']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_rows)
        print(f"   💾 Updated Shalom CSV with {len(csv_rows)} entries.")
    except Exception as e:
        print(f"   ❌ Failed to update Shalom CSV: {e}")

def load_archive_videos(church_name):
    csv_filename = f"{church_name.replace(' ', '_')}_archive_sermon_data.csv"
    paths = [os.path.join(DATA_DIR, csv_filename), csv_filename]
    archive_videos = []
    valid_path = None
    for p in paths:
        if os.path.exists(p):
            valid_path = p; break
    if valid_path:
        print(f"   📂 Loading Archive CSV: {valid_path}")
        try:
            with open(valid_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get("YouTube URL", "")
                    if "youtube.com" in url or "youtu.be" in url:
                        if "v=" in url: vid = url.split("v=")[1].split("&")[0]
                        else: vid = url.split("/")[-1].split("?")[0]
                        video_obj = {
                            'videoId': vid,
                            'title': {'runs': [{'text': row.get("Sermon Title", "Unknown Title")}]},
                            'manual_date': row.get("Sermon Date"),
                            'manual_speaker': row.get("Speaker")
                        }
                        archive_videos.append(video_obj)
        except Exception as e: print(f"   ⚠️ Error reading archive CSV: {e}")
    return archive_videos

def get_summary_file_path(church_name, ext=".csv"):
    return os.path.join(DATA_DIR, f"{church_name.replace(' ', '_')}_Summary{ext}")

def load_summary_history_csv(church_name):
    csv_path = get_summary_file_path(church_name, ".csv")
    history = {}
    if not os.path.exists(csv_path): return history
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader: history[row['url']] = row
    except: pass
    return history

def update_file_header_and_name(channel_dir, old_entry, new_speaker, new_date, title, church_name, language, video_type):
    old_safe_title = sanitize_filename(old_entry['title'])
    old_safe_speaker = sanitize_filename(old_entry['speaker'])
    old_date = old_entry['date']
    old_filename = f"{old_date} - {old_safe_title} - {old_safe_speaker}.txt"
    old_filepath = os.path.join(channel_dir, old_filename)
    new_safe_title = sanitize_filename(title)
    new_safe_speaker = sanitize_filename(new_speaker)
    new_filename = f"{new_date} - {new_safe_title} - {new_safe_speaker}.txt"
    new_filepath = os.path.join(channel_dir, new_filename)
    if os.path.exists(old_filepath):
        with open(old_filepath, 'r', encoding='utf-8') as f: content = f.read()
        content = re.sub(r'Speaker:.*', f'Speaker: {new_speaker}', content)
        content = re.sub(r'Date:.*', f'Date:    {new_date}', content)
        content = re.sub(r'START OF FILE:.*', f'START OF FILE: {new_filename}', content)
        if "Type:" not in content:
            content = content.replace(f"Church:  {church_name}\n", f"Church:  {church_name}\nType:    {video_type}\nLanguage:{language}\n")
        else:
            content = re.sub(r'Type:.*', f'Type:    {video_type}', content)
            content = re.sub(r'Language:.*', f'Language:{language}', content)
        with open(new_filepath, 'w', encoding='utf-8') as f: f.write(content)
        if old_filepath != new_filepath: os.remove(old_filepath)
        print(f"   🔄 Updated/Renamed file: {new_filename}")
        return True
    return False

def clean_history_of_bad_speakers(history):
    cleaned_history = {}
    bad_count = 0
    for url, entry in history.items():
        speaker = entry.get('speaker', '')
        # Basic check
        if "Brother " in speaker or "Sister " in speaker:
            bad_count += 1
            cleaned_history[url] = entry
        elif not is_valid_person_name(speaker):
            bad_count += 1
        else:
            cleaned_history[url] = entry
    if bad_count > 0:
        print(f"   🧹 Removed/Flagged {bad_count} corrupt entries from history.")
    return cleaned_history

def consolidate_names(found_speakers):
    if not found_speakers: return []
    sorted_names = sorted(list(found_speakers), key=len, reverse=True)
    final_names = []
    for name in sorted_names:
        is_duplicate = False
        for existing in final_names:
            if name in existing and name != existing:
                is_duplicate = True; break
        if not is_duplicate: final_names.append(name)
    return sorted(final_names)
    
def deep_clean_speakers_list(speakers_set):
    cleaned_set = set()
    suffixes_to_strip = [
        " Alpha", " Elijah", " The Parallel", " In Process", 
        " La Vida", " In The", " The",
        " – Tucson Tabernacle", " - Tucson Tabernacle", "\u2013 Tucson Tabernacle"
    ]
    for raw_entry in speakers_set:
        individual_names = split_multiple_speakers(raw_entry)
        for name in individual_names:
            name = clean_name(name)
            for suffix in suffixes_to_strip:
                if name.endswith(suffix): 
                    name = name.replace(suffix, "").strip()
            name = normalize_speaker(name)
            if is_valid_person_name(name): 
                cleaned_set.add(name)
    return cleaned_set

def process_channel(church_name, config, known_speakers, limit=None, recent_only=False):
    channel_url = config['url']
    clean_channel_name = church_name.replace(' ', '_')
    channel_dir = os.path.join(DATA_DIR, clean_channel_name)
    os.makedirs(channel_dir, exist_ok=True)

    print(f"\n--------------------------------------------------")
    print(f"Processing Channel: {church_name}")
    if limit:
        print(f"   📊 Scan Limit: Most recent {limit} videos.")
    elif recent_only:
        print(f"   📊 Scan Limit: Videos in the last 24 hours.")
    else:
        print(f"   📊 Scan Limit: FULL ARCHIVE.")
    time.sleep(1)

    history = load_summary_history_csv(church_name)
    history = clean_history_of_bad_speakers(history)

    base_channel_url = channel_url.split('/streams')[0].split('/videos')[0].split('/featured')[0]
    all_videos = []
    
    # --- SHALOM TABERNACLE SPECIFIC LOGIC ---
    if church_name == "Shalom Tabernacle" or church_name == "Shalom Tabernacle Tucson":
        print(f"   🔍 Detecting Shalom Tabernacle... Engaging Hybrid Mode.")
        csv_path = os.path.join(DATA_DIR, SHALOM_CSV_NAME)
        if not os.path.exists(csv_path) and os.path.exists(SHALOM_CSV_NAME):
            csv_path = SHALOM_CSV_NAME
        csv_videos = load_shalom_csv(csv_path)
        print(f"   📚 Loaded {len(csv_videos)} videos from CSV database.")
        
        web_max = 3 if limit else 3 
        print("   🌐 Scraping website for new uploads (First 3 pages)...")
        web_videos = st_scraper.fetch_sermons(max_pages=3)
        print(f"   🌐 Found {len(web_videos)} videos on recent pages.")
        
        merged_map = {}
        for v in csv_videos:
            merged_map[v['videoId']] = v
        new_count = 0
        for v in web_videos:
            if v['videoId'] not in merged_map:
                new_count += 1
                merged_map[v['videoId']] = v
        all_videos = list(merged_map.values())
        print(f"   ✨ Merged Total: {len(all_videos)} videos ({new_count} new found).")
        if new_count > 0:
            print(f"   💾 New videos detected! Updating {SHALOM_CSV_NAME}...")
            update_shalom_csv(csv_path, all_videos)
    else:
        archive_videos = load_archive_videos(church_name)
        if archive_videos:
            print(f"   📂 Added {len(archive_videos)} archive videos to queue.")
            all_videos.extend(archive_videos)
        try:
            print(f"   🔍 Scanning YouTube for videos...")
            all_videos.extend(list(scrapetube.get_channel(channel_url=base_channel_url, content_type='streams', limit=limit)))
            all_videos.extend(list(scrapetube.get_channel(channel_url=base_channel_url, content_type='videos', limit=limit)))
        except Exception as e:
            print(f"   ⚠️ Scrape Error: {e}")

    unique_videos_map = {}
    for v in all_videos:
        if 'manual_date' not in v: unique_videos_map[v['videoId']] = v
    for v in all_videos:
        if 'manual_date' in v: unique_videos_map[v['videoId']] = v

    unique_videos = unique_videos_map.values()
    print(f"   Videos found: {len(unique_videos)}")
    
    if len(unique_videos) == 0:
        print("   ❌ SKIPPING: No videos found.")
        return

    current_summary_list = [] 
    count = 0
    total = len(unique_videos)
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")

    for video in unique_videos:
        count += 1
        video_id = video['videoId']
        video_url = f"https://www.youtube.com/watch?v={video['videoId']}"
        try: title = video['title']['runs'][0]['text']
        except: title = "Unknown Title"
        
        # Check if the video is recent enough to be processed
        if recent_only and church_name != "Shalom Tabernacle" and church_name != "Shalom Tabernacle Tucson":
            published_time_text = video.get('publishedTimeText', {}).get('simpleText', '')
            if not parse_published_time(published_time_text):
                continue # Skip if the video is older than 24 hours

        manual_date = video.get('manual_date')
        manual_speaker = video.get('manual_speaker')

        if manual_speaker:
            speaker = normalize_speaker(manual_speaker)
            if speaker not in known_speakers:
                known_speakers.add(speaker)
                save_json_file(SPEAKERS_FILE, known_speakers)
        else:
            speaker, is_new = identify_speaker_dynamic(title, "", known_speakers)
            speaker = normalize_speaker(speaker)
            speaker = clean_name(speaker) 
            if is_new:
                print(f"   🎉 LEARNED NEW SPEAKER: {speaker}")
                save_json_file(SPEAKERS_FILE, known_speakers)

        video_type = determine_video_type(title, speaker)
        if video_type == "Memorial Service" and speaker != "William M. Branham":
            speaker = "Unknown Speaker"

        history_entry = history.get(video_url)
        needs_download = True
        
        if history_entry:
            old_speaker = history_entry.get('speaker', 'Unknown Speaker')
            old_status = history_entry.get('status', '')
            old_date = history_entry.get('date', 'Unknown Date')
            old_lang = history_entry.get('language', 'Unknown')
            
            is_bad_date = False
            try:
                if old_date and old_date != "Unknown Date":
                    dt_check = datetime.datetime.strptime(old_date, "%Y-%m-%d")
                    if dt_check.year < 2005: is_bad_date = True
            except: pass

            if old_lang == "Unknown" or old_lang == "": needs_download = True 
            elif is_bad_date: needs_download = True 
            elif speaker != "Unknown Speaker" and speaker != old_speaker:
                print(f"[{count}/{total}] 📝 CORRECTING SPEAKER: {old_speaker} -> {speaker}")
                if old_speaker in known_speakers:
                    known_speakers.remove(old_speaker)
                    save_json_file(SPEAKERS_FILE, known_speakers)
                if old_lang != "Unknown":
                    update_file_header_and_name(channel_dir, history_entry, speaker, old_date, title, church_name, old_lang, video_type)
                    needs_download = False 
                else: needs_download = True
            elif old_status == 'Success':
                history_entry['title'] = title
                history_entry['type'] = video_type
                history_entry['last_checked'] = today_str
                current_summary_list.append(history_entry)
                continue
            else:
                try:
                    last_checked_dt = datetime.datetime.strptime(history_entry.get('last_checked', '1900-01-01'), "%Y-%m-%d").date()
                    video_dt = datetime.datetime.strptime(old_date, "%Y-%m-%d").date() if old_date != "Unknown Date" else datetime.date(1900,1,1)
                    if (datetime.date.today() - video_dt).days > RECENT_VIDEO_THRESHOLD_DAYS and (datetime.date.today() - last_checked_dt).days < OLD_VIDEO_CHECK_INTERVAL_DAYS:
                        history_entry['title'] = title
                        history_entry['type'] = video_type
                        current_summary_list.append(history_entry)
                        continue
                except: pass

        if not needs_download and history_entry:
            history_entry['speaker'] = speaker
            history_entry['title'] = title
            history_entry['type'] = video_type
            history_entry['last_checked'] = today_str
            current_summary_list.append(history_entry)
            continue

        print(f"[{count}/{total}] PROCESSING: {title}")
        try:
            time.sleep(random.uniform(5, 12)) 
            transcript_text, description, yt_obj = get_transcript_data(video_id)
            if manual_date: sermon_date = manual_date
            else: sermon_date = determine_sermon_date(title, description, yt_obj)
            language = determine_language(title, yt_obj)
            if speaker == "Unknown Speaker":
                speaker, _ = identify_speaker_dynamic(title, description, known_speakers)
                speaker = normalize_speaker(speaker)
                speaker = clean_name(speaker)
            
            video_type = determine_video_type(title, speaker)
            if video_type == "Memorial Service" and speaker != "William M. Branham":
                speaker = "Unknown Speaker"

            status = "Success"
            if not transcript_text:
                status = "No Transcript"
                print(f"   ❌ No Transcript found (Lang: {language}).")
            else:
                safe_title = sanitize_filename(title)
                safe_speaker = sanitize_filename(speaker)
                filename = f"{sermon_date} - {safe_title} - {safe_speaker}.txt"
                filepath = os.path.join(channel_dir, filename)
                if not os.path.exists(filepath):
                    entry = format_sermon_entry(video_id, title, sermon_date, transcript_text, church_name, speaker, language, video_type)
                    with open(filepath, 'a', encoding='utf-8') as f: f.write(entry)
                    print(f"   ✅ Transcript downloaded & Saved (Lang: {language}).")
                else:
                    entry = format_sermon_entry(video_id, title, sermon_date, transcript_text, church_name, speaker, language, video_type)
                    with open(filepath, 'w', encoding='utf-8') as f: f.write(entry)
                    print(f"   ✅ File updated.")

            current_summary_list.append({
                "date": sermon_date, "status": status, "speaker": speaker,
                "title": title, "url": video_url, "last_checked": today_str,
                "language": language, "type": video_type
            })
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
            current_summary_list.append({
                "date": "Error", "status": "Failed", "speaker": "Unknown",
                "title": title, "url": video_url, "last_checked": today_str,
                "language": "Unknown", "type": "Unknown"
           
            })

    csv_path = get_summary_file_path(church_name, ".csv")
    # --- NEW LOGIC: Ensure every .txt transcript is represented in the summary CSV ---
    channel_dir = os.path.join(DATA_DIR, church_name.replace(' ', '_'))
    txt_files = [f for f in os.listdir(channel_dir) if f.endswith('.txt')]
    # Build a set of (date, title, speaker) from current_summary_list for fast lookup
    summary_keys = set()
    for entry in current_summary_list:
        key = (entry.get('date', '').strip(), entry.get('title', '').strip(), entry.get('speaker', '').strip())
        summary_keys.add(key)
    # For each .txt file, parse metadata and add to summary if missing
    for txt_file in txt_files:
        # Try to parse: YYYY-MM-DD - Title - Speaker.txt
        base = os.path.splitext(txt_file)[0]
        parts = base.split(' - ')
        if len(parts) >= 3:
            date, title, speaker = parts[0].strip(), parts[1].strip(), parts[2].strip()
        else:
            # Fallback: use filename as title, unknown for others
            date, title, speaker = '', base, ''
        # Check if already in summary
        if (date, title, speaker) not in summary_keys:
            # Try to extract more info from file content if needed
            filepath = os.path.join(channel_dir, txt_file)
            language = 'Unknown'
            video_type = 'Full Sermon'
            url = ''
            last_checked = datetime.datetime.now().strftime("%Y-%m-%d")
            status = 'Success'
            # Try to find YouTube URL in file
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read(2048)
                    match = re.search(r'(https://www\.youtube\.com/watch\?v=[\w-]+)', content)
                    if match:
                        url = match.group(1)
            except Exception:
                pass
            current_summary_list.append({
                "date": date,
                "status": status,
                "speaker": speaker,
                "title": title,
                "url": url,
                "last_checked": last_checked,
                "language": language,
                "type": video_type
            })
            summary_keys.add((date, title, speaker))
    # Write the updated summary CSV
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["date", "status", "speaker", "title", "url", "last_checked", "language", "type"])
            writer.writeheader()
            writer.writerows(current_summary_list)
    except Exception as e:
        print(f"   ❌ Error writing summary CSV: {e}")
    
    save_json_file(SPEAKERS_FILE, known_speakers)
    print(f"SUCCESS: {church_name} complete.")

def main():
    prevent_sleep()
    try:
        parser = argparse.ArgumentParser(description="Update sermon transcripts from YouTube channels.")
        parser.add_argument('--recent', action='store_true', help="Only process videos uploaded in the last 24 hours.")
        parser.add_argument('--heal', action='store_true', help="Only run the heal archive process.")
        parser.add_argument('--force', action='store_true', help="Force re-processing of all files during healing.")
        args = parser.parse_args()

        if args.heal:
            print("Running deep archive healing & cleanup...")
            heal_archive(DATA_DIR, force=args.force)
            return

        channels = load_config()
        if not channels:
            print("No channels found in channels.json.")
            return

        # Startup clean
        raw_speakers = load_json_file(SPEAKERS_FILE)
        known_speakers = deep_clean_speakers_list(raw_speakers)
        save_json_file(SPEAKERS_FILE, known_speakers)
        
        # When running with --recent, we don't need a menu.
        if args.recent:
            for name, config in channels.items():
                process_channel(name, config, known_speakers, recent_only=True)
            print("\n✅ Recent scrape complete. Running Post--Scrape Self-Healing...")
            heal_archive(DATA_DIR)
            return

        # --- MENU ---
        print("\n" + "="*50)
        print("ACTION SELECTION")
        print("="*50)
        print(" 1. Single Channel Scrape")
        print(" 2. All Channels Scrape")
        print(" 3. Run Deep Self-Healing & Cleanup (No Scraping)")
        print("="*50)
        
        action = input("\n👉 Enter Number: ").strip()
        
        if action == '3':
            heal_archive(DATA_DIR)
            return

        # --- CHANNEL SELECTION ---
        channel_name = ""
        if action == '1':
            channel_name = input("Enter Channel Name: ").strip()
            if channel_name in channels:
                process_channel(channel_name, channels[channel_name], known_speakers)
            else:
                print("Channel not found!")
        elif action == '2':
            for name, config in channels.items():
                process_channel(name, config, known_speakers)
        else:
            print("Invalid action. Exiting.")
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback

        traceback.print_exc()

# === Ensure this is at the very end of the file ===
print("=== update_sermons.py script started ===")
if __name__ == "__main__":
    main()