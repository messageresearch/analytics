import os
import re
import json
import datetime
import argparse
import scrapetube
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import TextFormatter

# --- CONFIGURATION ---
CONFIG_FILE = "channels.json"
DATA_DIR = "data"  # Folder to store the text files

def load_config():
    if not os.path.exists(CONFIG_FILE):
        print(f"Config file {CONFIG_FILE} not found.")
        return {}
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def get_existing_video_ids(filepath):
    if not os.path.exists(filepath):
        return set()
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    # Matches: youtube.com/watch?v=ID
    ids = set(re.findall(r'youtube\.com\/watch\?v=([a-zA-Z0-9_-]{11})', content))
    return ids

def format_sermon_entry(video_id, title, date_str, transcript_text, church_name):
    # Basic speaker guessing logic based on title keywords
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

def process_channel(church_name, config, limit=10):
    channel_url = config['url']
    filename = config['filename']
    filepath = os.path.join(DATA_DIR, filename)
    
    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    print(f"--- Processing {church_name} ---")
    print(f"Target File: {filepath}")

    existing_ids = get_existing_video_ids(filepath)
    print(f"Found {len(existing_ids)} existing videos.")

    # Determine content type (streams vs videos) based on URL structure
    c_type = 'streams' if '/streams' in channel_url else 'videos'
    
    try:
        videos = scrapetube.get_channel(channel_url=channel_url, content_type=c_type)
    except Exception as e:
        print(f"Error fetching channel: {e}")
        return

    new_entries = []
    count = 0
    # Use current date as fallback since exact YouTube upload date is hard to get without API key
    fallback_date = datetime.datetime.now().strftime("%Y-%m-%d")

    for video in videos:
        if count >= limit: break
        count += 1
        
        video_id = video['videoId']
        
        # Safely extract title
        try:
            title = video['title']['runs'][0]['text']
        except:
            title = "Unknown Title"
        
        if video_id in existing_ids:
            continue

        print(f"New sermon found: {title} ({video_id})")

        try:
            # 1. Fetch Transcript Object
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
            
            # 2. Try to get manual English, fallback to auto-generated
            try:
                transcript = transcript_list.find_manually_created_transcript(['en'])
            except:
                transcript = transcript_list.find_generated_transcript(['en'])
            
            # 3. Download the actual text data
            transcript_data = transcript.fetch()
            
            # 4. Format to plain text
            formatter = TextFormatter()
            text_formatted = formatter.format_transcript(transcript_data)
            
            # 5. Build the text block
            entry = format_sermon_entry(video_id, title, fallback_date, text_formatted, church_name)
            new_entries.append(entry)
            
        except Exception as e:
            print(f"Skipping {video_id} (No transcript available or Error): {e}")

    # Write new entries to file (Oldest first if multiple found)
    if new_entries:
        with open(filepath, 'a', encoding='utf-8') as f:
            for entry in reversed(new_entries):
                f.write(entry)
        print(f"Successfully added {len(new_entries)} new sermons to {filename}.")
    else:
        print(f"No new sermons found for {church_name}.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=10, help='Max videos to check per channel')
    args = parser.parse_args()

    channels = load_config()
    
    if not channels:
        print("No channels found in channels.json. Please create it.")
        return

    for name, config in channels.items():
        process_channel(name, config, args.limit)

if __name__ == "__main__":
    main()