import os
import re
import json
import datetime
import argparse
import random
import time
import requests
import xml.etree.ElementTree as ET
import scrapetube
from pytubefix import YouTube
from pytubefix.cli import on_progress

# --- CONFIGURATION ---
CONFIG_FILES = ["channels.json", "config.json"]
DATA_DIR = "data"

# Standard Browser User-Agents (From your working script)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
]

def get_random_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.youtube.com/",
    }

def load_config():
    for config_file in CONFIG_FILES:
        if os.path.exists(config_file):
            print(f"Loading configuration from: {config_file}")
            with open(config_file, 'r') as f:
                return json.load(f)
    print(f"ERROR: No configuration file found. Checked: {CONFIG_FILES}")
    return {}

def get_existing_video_ids(filepath):
    if not os.path.exists(filepath):
        return set()
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    ids = set(re.findall(r'youtube\.com\/watch\?v=([a-zA-Z0-9_-]{11})', content))
    return ids

def format_sermon_entry(video_id, title, date_str, transcript_text, church_name):
    speaker = "Unknown Speaker"
    if "Evans" in title: speaker = "Brother Daniel Evans"
    elif "Brisson" in title: speaker = "Brother Steeve Brisson"
    elif "Guerra" in title: speaker = "Brother Aaron Guerra"
    elif "Branham" in title: speaker = "Brother William Branham"
    
    header = f"""
################################################################################
START OF FILE: {date_str} - {title} - {speaker} - Clean.txt
################################################################################

SERMON DETAILS
========================================
Date:    {date_str}
Title:   {title}
Speaker: {speaker}
Church:  {church_name}
URL:     https://www.youtube.com/watch?v={video_id}
========================================

"""
    return header + transcript_text + "\n"

def xml_to_text(xml_content):
    """
    Parses the raw XML transcript from YouTube into clean text.
    """
    try:
        root = ET.fromstring(xml_content)
        clean_lines = []
        
        for child in root:
            if child.tag == 'text':
                text = child.text or ""
                # Decode HTML entities manually just in case
                text = text.replace('&nbsp;', ' ').replace('&#39;', "'").replace('&quot;', '"').replace('&amp;', '&')
                text = " ".join(text.split())
                
                if text:
                    clean_lines.append(text)
        return " ".join(clean_lines)
    except Exception as e:
        print(f"XML Parsing Error: {e}")
        return None

def get_transcript_text(video_id):
    """
    Fetches transcript using the side-channel method (requests + spoofed headers)
    adapted from transcript_scraper.py.
    """
    url = f"https://www.youtube.com/watch?v={video_id}"
    
    # Initialize YouTube object
    # Note: verify if use_oauth=True is strictly necessary for your specific videos. 
    # Usually False is better for CI/CD unless you have age-gated content.
    yt = YouTube(url, use_oauth=False, allow_oauth_cache=False, on_progress_callback=on_progress)
    
    # 1. Get the Caption Track Object
    try:
        # Trigger title fetch to ensure metadata is loaded
        _ = yt.title 
        
        caption_track = None
        # Priority search for English captions
        if 'en' in yt.captions: caption_track = yt.captions['en']
        elif 'a.en' in yt.captions: caption_track = yt.captions['a.en']
        elif 'en-US' in yt.captions: caption_track = yt.captions['en-US']
        
        # Fallback search
        if not caption_track:
            for code in yt.captions:
                if code.code.startswith('en'):
                    caption_track = yt.captions[code]
                    break
        
        if not caption_track:
             # Debugging: List available codes
            available = [c.code for c in yt.captions]
            raise Exception(f"No English captions found. Available: {available}")

        # 2. Download Content (Side-Channel Request)
        # This is the secret sauce from your working script
        print(f"   Downloading caption XML from: {caption_track.url[:50]}...")
        response = requests.get(caption_track.url, headers=get_random_headers())
        
        if response.status_code == 200:
            clean_text = xml_to_text(response.text)
            if not clean_text:
                raise Exception("Transcript downloaded but parsed empty.")
            return clean_text
        elif response.status_code == 429:
            raise Exception("Rate Limit (429) encountered during caption download.")
        else:
            raise Exception(f"HTTP Error {response.status_code} fetching caption.")

    except Exception as e:
        raise e

def process_channel(church_name, config, limit=10):
    channel_url = config['url']
    filename = config['filename']
    filepath = os.path.join(DATA_DIR, filename)
    
    os.makedirs(DATA_DIR, exist_ok=True)

    print(f"\n--------------------------------------------------")
    print(f"Processing Channel: {church_name}")
    
    # Clean URL to get base channel
    base_channel_url = channel_url.split('/streams')[0].split('/videos')[0].split('/featured')[0]
    print(f"Base URL: {base_channel_url}")
    
    existing_ids = get_existing_video_ids(filepath)
    print(f"Found {len(existing_ids)} existing videos.")

    all_videos = []
    
    # Scan Streams tab
    try:
        print("Scanning 'streams'...")
        streams = list(scrapetube.get_channel(channel_url=base_channel_url, content_type='streams', limit=limit))
        print(f"Found {len(streams)} streams.")
        all_videos.extend(streams)
    except: pass

    # Scan Videos tab
    try:
        print("Scanning 'videos'...")
        uploads = list(scrapetube.get_channel(channel_url=base_channel_url, content_type='videos', limit=limit))
        print(f"Found {len(uploads)} videos.")
        all_videos.extend(uploads)
    except: pass

    # Deduplicate
    unique_videos = {v['videoId']: v for v in all_videos}.values()
    
    if not unique_videos:
        print(f"⚠️ No videos found for {church_name}. Check URL.")
        return

    print(f"Total unique videos to check: {len(unique_videos)}")
    
    new_entries = []
    fallback_date = datetime.datetime.now().strftime("%Y-%m-%d")

    for video in unique_videos:
        video_id = video['videoId']
        try: title = video['title']['runs'][0]['text']
        except: title = "Unknown Title"
        
        if video_id in existing_ids:
            continue

        print(f"NEW CONTENT FOUND: {title} ({video_id})")

        try:
            # Add a small delay to be polite to YouTube API
            time.sleep(random.uniform(1, 3))
            
            text_formatted = get_transcript_text(video_id)
            entry = format_sermon_entry(video_id, title, fallback_date, text_formatted, church_name)
            new_entries.append(entry)
            print(f"✅ Transcript downloaded.")
            
        except Exception as e:
            print(f"❌ Skipping {video_id}: {str(e)}")

    if new_entries:
        print(f"Writing {len(new_entries)} new sermons to {filepath}...")
        with open(filepath, 'a', encoding='utf-8') as f:
            for entry in reversed(new_entries):
                f.write(entry)
        print(f"SUCCESS: {filename} updated.")
    else:
        print(f"No new transcripts for {church_name}.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=10)
    args = parser.parse_args()

    channels = load_config()
    if not channels: return

    for name, config in channels.items():
        process_channel(name, config, args.limit)

if __name__ == "__main__":
    main()