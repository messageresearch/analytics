import os
import re
import json
import datetime
import argparse
import scrapetube
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import TextFormatter

# --- CONFIGURATION ---
CONFIG_FILE = "channels.json" # Ensure this matches your actual file name (config.json vs channels.json)
DATA_DIR = "data"

def load_config():
    # Check for both common names just in case
    if os.path.exists("config.json"):
        with open("config.json", 'r') as f: return json.load(f)
    if os.path.exists("channels.json"):
        with open("channels.json", 'r') as f: return json.load(f)
    
    print("Config file (config.json or channels.json) not found.")
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
    # Customize speaker guessing per church if needed, or generic
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
    
    os.makedirs(DATA_DIR, exist_ok=True)

    print(f"--- Processing {church_name} ---")
    
    existing_ids = get_existing_video_ids(filepath)
    print(f"Found {len(existing_ids)} existing videos in {filename}.")

    # 1. Try fetching based on URL type first
    c_type = 'streams' if '/streams' in channel_url else 'videos'
    print(f"Scraping {c_type} from {channel_url}...")
    
    videos = list(scrapetube.get_channel(channel_url=channel_url, content_type=c_type, limit=limit))

    # 2. Fallback: If 0 videos found, try the OTHER type (e.g. if streams empty, try videos)
    if not videos:
        fallback_type = 'videos' if c_type == 'streams' else 'streams'
        print(f"No {c_type} found. Trying {fallback_type}...")
        # We need the base channel URL without /streams or /videos
        base_url = channel_url.split('/streams')[0].split('/videos')[0]
        videos = list(scrapetube.get_channel(channel_url=base_url, content_type=fallback_type, limit=limit))

    if not videos:
        print(f"No videos found for {church_name} (checked streams and uploads).")
        return

    print(f"Found {len(videos)} recent videos on YouTube.")
    
    new_entries = []
    fallback_date = datetime.datetime.now().strftime("%Y-%m-%d")

    for video in videos:
        video_id = video['videoId']
        try:
            title = video['title']['runs'][0]['text']
        except:
            title = "Unknown Title"
        
        if video_id in existing_ids:
            continue

        print(f"New sermon found: {title} ({video_id})")

        try:
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
            try:
                transcript = transcript_list.find_manually_created_transcript(['en'])
            except:
                transcript = transcript_list.find_generated_transcript(['en'])
            
            transcript_data = transcript.fetch()
            formatter = TextFormatter()
            text_formatted = formatter.format_transcript(transcript_data)
            
            entry = format_sermon_entry(video_id, title, fallback_date, text_formatted, church_name)
            new_entries.append(entry)
            
        except Exception as e:
            # Common error: Transcripts are disabled for this video
            print(f"Skipping {video_id}: {e}")

    if new_entries:
        with open(filepath, 'a', encoding='utf-8') as f:
            for entry in reversed(new_entries):
                f.write(entry)
        print(f"Successfully added {len(new_entries)} new sermons to {filename}.")
    else:
        print(f"No new transcripts available for {church_name}.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=10, help='Max videos to check per channel')
    args = parser.parse_args()

    channels = load_config()
    
    if not channels:
        print("No channels found in config.json or channels.json.")
        return

    for name, config in channels.items():
        process_channel(name, config, args.limit)

if __name__ == "__main__":
    main()