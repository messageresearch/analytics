
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
from pytubefix import YouTube, Playlist
from pytubefix.cli import on_progress

# Import WOLJC speaker scraper for post-processing Word of Life Church videos
try:
    from woljc_speaker_scraper import update_speakers_for_videos as woljc_update_speakers
except ImportError:
    woljc_update_speakers = None

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

# --- SPEAKER DETECTION LOG FUNCTION ---
def write_speaker_detection_log(stats_dict, operation_name="Speaker Detection"):
    """
    Write speaker detection statistics to a timestamped log file in the data folder.
    
    Args:
        stats_dict: Dictionary containing statistics like:
            - total_processed: Total number of entries processed
            - speakers_detected: Number of entries with detected speakers
            - unknown_speakers: Number of entries with unknown speakers
            - transcripts_updated: Number of transcript files updated (optional)
            - summaries_updated: Number of summary CSV entries updated (optional)
            - skipped_same: Number skipped because speaker was same (optional)
            - skipped_not_found: Number skipped because file not found (optional)
            - by_church: Dict of church-level stats (optional)
        operation_name: Name of the operation for the log header
    """
    timestamp = datetime.datetime.now()
    filename = f"speaker_detection_log_{timestamp.strftime('%Y%m%d_%H%M%S')}.txt"
    filepath = os.path.join(DATA_DIR, filename)
    
    total = stats_dict.get('total_processed', 0)
    detected = stats_dict.get('speakers_detected', 0)
    unknown = stats_dict.get('unknown_speakers', 0)
    
    # Calculate detection rate
    detection_rate = (detected / total * 100) if total > 0 else 0
    
    lines = [
        "=" * 70,
        f"SPEAKER DETECTION LOG - {operation_name}",
        f"Generated: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 70,
        "",
        "SUMMARY STATISTICS",
        "-" * 40,
        f"Total Entries Processed:    {total:,}",
        f"Speakers Detected:          {detected:,}",
        f"Unknown Speakers:           {unknown:,}",
        f"Detection Rate:             {detection_rate:.1f}%",
        "",
    ]
    
    # Add optional stats if present
    if 'speakers_corrected' in stats_dict:
        lines.append(f"Speakers Corrected:         {stats_dict['speakers_corrected']:,}")
    if 'speakers_redetected' in stats_dict:
        lines.append(f"Speakers Re-detected:       {stats_dict['speakers_redetected']:,}")
    if 'transcripts_updated' in stats_dict:
        lines.append(f"Transcripts Updated:        {stats_dict['transcripts_updated']:,}")
    if 'summaries_updated' in stats_dict:
        lines.append(f"Summary Entries Updated:    {stats_dict['summaries_updated']:,}")
    if 'skipped_same' in stats_dict:
        lines.append(f"Skipped (Same Speaker):     {stats_dict['skipped_same']:,}")
    if 'skipped_not_found' in stats_dict:
        lines.append(f"Skipped (File Not Found):   {stats_dict['skipped_not_found']:,}")
    
    # Add church-level breakdown if present
    if 'by_church' in stats_dict and stats_dict['by_church']:
        lines.extend([
            "",
            "BY CHURCH BREAKDOWN",
            "-" * 40,
        ])
        for church, church_stats in sorted(stats_dict['by_church'].items()):
            church_total = church_stats.get('total', 0)
            church_detected = church_stats.get('detected', 0)
            church_unknown = church_stats.get('unknown', 0)
            church_rate = (church_detected / church_total * 100) if church_total > 0 else 0
            lines.append(f"{church}:")
            lines.append(f"    Total: {church_total:,}  |  Detected: {church_detected:,}  |  Unknown: {church_unknown:,}  |  Rate: {church_rate:.1f}%")
    
    # Add new speakers discovered if present
    if 'new_speakers' in stats_dict and stats_dict['new_speakers']:
        lines.extend([
            "",
            "NEW SPEAKERS DISCOVERED",
            "-" * 40,
        ])
        for speaker in sorted(stats_dict['new_speakers']):
            lines.append(f"  • {speaker}")
    
    lines.extend([
        "",
        "=" * 70,
        "END OF LOG",
        "=" * 70,
    ])
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        print(f"\n📄 Log saved to: {filepath}")
        return filepath
    except Exception as e:
        print(f"⚠️ Error writing log file: {e}")
        return None

# --- IMPROVED SPEAKER DETECTION (from speaker_detector.py) ---
# Known speakers - first names mapped to full names (for completing partial detections)
KNOWN_SPEAKERS_MAP = {
    'Faustin': 'Faustin Lukumuena',
    'Theo': 'Theo Ovid',
    'Busobozi': 'Busobozi Talemwa',
}

# Common honorific prefixes used in sermon titles
HONORIFICS = [
    r'Bro\.?', r'Brother', r'Sis\.?', r'Sister', r'Pastor', r'Pr\.?', r'Rev\.?', 
    r'Reverend', r'Elder', r'Deacon', r'Minister', r'Dr\.?', r'Bishop', 
    r'Apostle', r'Evangelist', r'Prophet', r'Hno\.?', r'Hermano', r'Hermana',
    r'Founder', r'Associate\s+Pastor', r'Ministering', r'Ministered\s+by',
    r'Br\.?', r'Sr\.?', r'Ptr\.?', r'Pst\.?', r'Past\.?', r'Founding\s+Pastor',
]
HONORIFIC_PATTERN = r'(?:' + '|'.join(HONORIFICS) + r')'

# Common words that indicate a speaker follows
SPEAKER_INDICATORS = [
    r'by', r'with', r'from', r'featuring', r'feat\.?', r'ft\.?', 
    r'ministered\s+by', r'preached\s+by', r'delivered\s+by',
    r'speaker[:\s]', r'minister[:\s]',
]

# Pattern for name-like sequences (capitalized words)
NAME_PATTERN = r"[A-Z][a-z]+(?:[\s'-][A-Z][a-z]+)*"

# Words/patterns that should NOT be considered names (expanded from speaker_detector.py)
NON_NAME_PATTERNS = [
    r'^Part\b', r'^Pt\.?\b', r'^Episode\b', r'^Ep\.?\b', r'^Vol\.?\b',
    r'^The\b', r'^A\b', r'^An\b', r'^This\b', r'^That\b', r'^Our\b', r'^My\b',
    r'^God\b', r'^Jesus\b', r'^Christ\b', r'^Lord\b', r'^Holy\b', r'^Spirit\b',
    r'^Sunday\b', r'^Monday\b', r'^Tuesday\b', r'^Wednesday\b', r'^Thursday\b', r'^Friday\b', r'^Saturday\b',
    r'^January\b', r'^February\b', r'^March\b', r'^April\b', r'^May\b', r'^June\b',
    r'^July\b', r'^August\b', r'^September\b', r'^October\b', r'^November\b', r'^December\b',
    r'^Morning\b', r'^Evening\b', r'^Night\b', r'^Service\b', r'^Meeting\b',
    r'^Live\b', r'^Livestream\b', r'^Prayer\b', r'^Worship\b', r'^Praise\b',
    r'^Camp\b', r'^Revival\b', r'^Conference\b', r'^Convention\b',
    r'^Full\b', r'^Gospel\b', r'^Sermon\b', r'^Message\b', r'^Word\b',
    r'^Church\b', r'^Tabernacle\b', r'^Temple\b', r'^Chapel\b',
    r'^Special\b', r'^Memorial\b', r'^Funeral\b', r'^Wedding\b',
    r'^Christmas\b', r'^Easter\b', r'^Thanksgiving\b', r'^New\s+Year',
    r'^\d', r'^[A-Z]{2,5}$',
    r'^Clip[:\s]?', r'^Youth\b', r'^Children\b', r'^School$',
    r'^Entering\b', r'^Possessors\b', r'^Walking\b', r'^Standing\b', r'^Seeking\b',
    r'^Living\b', r'^Being\b', r'^Having\b', r'^Getting\b', r'^Finding\b',
    r'^Knowing\b', r'^Understanding\b', r'^Believing\b', r'^Trusting\b',
    r'^Coming\b', r'^Going\b', r'^Turning\b', r'^Following\b', r'^Looking\b',
    r'^Pressing\b', r'^Holding\b', r'^Keeping\b', r'^Fighting\b', r'^Running\b',
    r'^Waiting\b', r'^Resting\b', r'^Abiding\b', r'^Dwelling\b', r'^Rising\b',
    r'^Breaking\b', r'^Building\b', r'^Growing\b', r'^Changing\b', r'^Moving\b',
    r'^Overcoming\b', r'^Victorious\b', r'^Triumphant\b', r'^Faithful\b',
    r'^Anointed\b', r'^Chosen\b', r'^Called\b', r'^Blessed\b', r'^Redeemed\b',
    r'^Taped\b', r'^Questions\b', r'^Answers\b',
]

# Date patterns to strip from potential names
DATE_PATTERNS = [
    r'\s+on\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)(?:\s+\d{1,2}(?:st|nd|rd|th)?)?(?:,?\s*\d{4})?',
    r'\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2}(?:st|nd|rd|th)?(?:,?\s*\d{4})?',
    r'\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b',
    r'\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b',
    r'\b\d{6}\b',
]

# Hardcoded speaker data for Tucson Tabernacle (TT) titles
# This data was scraped from the Tucson Tabernacle website, not YouTube
TUCSON_TABERNACLE_SPEAKERS = {
    "TT Friday December 16th, 2022 7 PM Service": "Joe Adams",
    "TT Sunday December 11th, 2022 10 AM Service": "Aaron Guerra",
    "TT Sunday November 27th, 2022 10 AM Service": "Aaron Guerra",
    "TT Sunday November 20th, 2022 5 PM Service": "Aaron Guerra",
    "TT Sunday November 20th, 2022 10 AM Service": "Daniel Evans",
    "TT Wednesday November 16th, 2022 7 PM Service": "Lyle Johnson",
    "TT Wednesday November 9th, 2022 7 PM Service": "Daniel Evans",
    "TT Sunday November 6th, 2022 5 PM Service": "Lyle Johnson",
    "TT Wednesday November 2nd, 2022 7 PM Service": "William M. Branham",
    "TT Sunday October 30th, 2022 10 AM Service": "Aaron Guerra",
    "TT Sunday October 30th, 2022 5 PM Service": "Aaron Guerra",
    "TT Wednesday October 26th, 2022 7 PM Service": "Lyle Johnson",
    "TT Wednesday October 19th, 2022 7 PM Service": "Peter McFadden",
    "TT Sunday October 16th, 2022 5 PM Service": "Noel Johnson",
    "TT Sunday October 16th, 2022 10 AM Service": "Daniel Evans",
    "TT Wednesday October 12th, 2022 7 PM Service": "William M. Branham",
    "TT Sunday October 9th, 2022 5 PM Service": "Matthew Watkins",
    "TT Sunday October 9th, 2022 10 AM Service": "Matthew Watkins",
    "TT Wednesday October 5th, 2022 7 PM Service": "Lyle Johnson",
    "TT Sunday October 2nd, 2022 5 PM Service": "Daniel Evans",
    "TT Sunday October 2nd, 2022 10 AM Service": "Peter McFadden",
    "TT Sunday September 25th, 2022 5 PM Service": "Reggie Plett",
    "TT Sunday September 25th, 2022 10 AM Service": "Reggie Plett",
    "TT Sunday September 18th, 2022 10 AM Service": "Aaron Guerra",
    "TT Wednesday Sep 14th, 2022 7 PM Service": "Peter McFadden",
    "TT Sunday September 11th, 2022 5 PM Service": "Lyle Johnson",
    "TT Wednesday Sep 7th, 2022 7 PM Service": "Peter McFadden",
    "TT Sunday September 4th, 2022 10 AM Service": "Daniel Evans",
    "TT Wednesday Aug 31st, 2022 7 PM Service": "Lyle Johnson",
    "TT Sunday August 28th, 2022 5 PM Service": "Joe Adams",
    "TT Wednesday Aug  24th, 2022 7 PM Service": "Peter McFadden",
    "TT Sunday Aug 14th, 2022 10 AM Service": "Matthew Watkins",
    "TT Sunday August 14th, 2022 5 PM Service": "Matthew Watkins",
    "TT Wednesday Aug  10th, 2022 7 PM Service": "William M. Branham",
    "TT Sunday August 7th, 2022 5 PM Service": "Lyle Johnson",
    "TT Sunday Aug 7th, 2022 10 AM Service": "Daniel Evans",
    "TT Sunday July 31st, 2022 10 AM Service": "Aaron Guerra",
    "TT Wednesday July 27th, 2022 7 PM Service": "William M. Branham",
    "TT Wednesday July 20th, 2022 7 PM Service": "William M. Branham",
    "TT Sunday July 17th, 2022 10 AM Service": "Peter McFadden",
    "TT Wednesday July 13th, 2022 7 PM Service": "Lyle Johnson",
    "TT Sunday July 10th, 2022 5 PM Service": "Daniel Evans",
    "TT Sunday July 3rd, 2022 10 AM Service": "Martin Warner",
    "TT Sunday June 26th, 2022 5 PM Service": "Matthew Watkins",
    "TT Sunday June 26th, 2022 10 AM Service": "Matthew Watkins",
    "TT Wednesday June 22nd, 2022 7 PM Service": "William M. Branham",
    "TT Wednesday June 15th, 2022 7 PM Service": "Lyle Johnson",
    "TT Sunday June 12th, 2022 10 AM Service": "Daniel Evans",
    "TT Wednesday June 8th, 2022 7 PM Service": "William M. Branham",
    "TT Sunday June 5th, 2022 10 AM Service": "Joe Adams",
    "TT Sunday June 5th, 2022 5 PM Service": "Joe Adams",
    "TT Wednesday June 1st, 2022 7 PM Service": "Robert Figueroa",
    "TT Sunday May 29th, 2022 5 PM Service": "Lyle Johnson",
    "TT Wednesday May 25th, 2022 7 PM Service": "Daniel Evans",
    "TT Sunday May 22nd, 2022 5 PM Service": "Reggie Plett",
    "TT Sunday May 15th, 2022 10 AM Service": "Isaiah Chisolm",
    "TT Sunday May 15th, 2022 5 PM Service": "Isaiah Chisolm",
    "TT Sunday May 8th, 2022 10 AM Service": "Aaron Guerra",
    "TT Wednesday May 4th, 2022 7 PM Service": "Robert Figueroa",
    "TT Sunday May 1st, 2022 10 AM Service": "Tim Cross",
    "TT Sunday April 24th, 2022 10 AM Service": "Daniel Evans",
    "TT Sunday April 17th, 2022 10 AM Service": "Daniel Evans",
    "TT Sunday April 10th, 2022 10 AM Service": "Aaron Guerra",
    "TT Wednesday April 6th, 2022 7 PM Service": "Lyle Johnson",
    "TT Sunday April 3th, 2022 10 AM Service": "Daniel Evans",
    "TT Sunday April 3rd, 2022 5 PM Service": "Robert Figueroa",
    "TT Wednesday March 23rd, 2022 7 PM Service": "William M. Branham",
    "TT Sunday March 20th, 2022 5 PM Service": "Peter McFadden",
    "TT Sunday March 20th, 2022 10 AM Service": "Aaron Guerra",
    "TT Sunday March 13th, 2022 10 AM Service": "Aaron Guerra",
    "TT Sunday March 13th, 2022 5 PM Service": "Jesse Wilson",
    "TT Sunday March 6th, 2022 5 PM Service": "Daniel Evans",
    "TT Wednesday March 2nd, 2022 7 PM Service": "Lyle Johnson",
    "TT Sunday February 27th, 2022 10 AM Service": "Bernhard Frank",
    "TT Sunday February 20th, 2022 5 PM Service": "Reggie Plett",
    "TT Sunday February 13th, 2022 5 PM Service": "Martin Warner",
    "TT Wednesday February 9th, 2022 7 PM Service": "William M. Branham",
    "TT Sunday February 6th, 2022 5 PM Service": "Peter McFadden",
    "TT Wednesday February 2nd, 2022 7 PM Service": "Lyle Johnson",
    "TT Sunday January 30th, 2022 10 AM Service": "Joe Adams",
    "TT Wednesday January 26th, 2022 7 PM Service": "Daniel Evans",
    "TT Sunday January 23rd 2022 10 AM Service": "Steve Shelley",
    "TT Sunday January 23rd, 2022 5 PM Service": "Steve Shelley",
    "TT Sunday January 16th, 2022 10 AM Service": "Tim Cross",
    "TT Wednesday January 5th, 2022 7 PM Service": "Lyle Johnson",
    "TT Wednesday December 29th, 2021 7 PM Service": "Daniel Evans",
    "TT Sunday December 26th, 2021 10 AM Service": "Aaron Guerra",
    "TT Sunday December 19th, 2021 5 PM Service": "Peter McFadden",
    "TT Sunday December 19th, 2021 10 AM Service": "Aaron Guerra",
    "TT Wednesday December 15th, 2021 7 PM Service": "Lyle Johnson",
    "TT Sunday December 12th, 2021 5 PM Service": "Tim Cross",
    "TT Sunday December 5th, 2021 5 PM Service": "Dale Smith",
    "TT Sunday November 21st, 2021 5 PM Service": "Joe Adams",
    "TT Wednesday November 10th, 2021 7 PM Service": "William M. Branham",
    "TT Sunday July 26th, 2020 10 AM Service": "Matthew Watkins",
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


def fetch_playlist_videos(playlist_id, limit=None):
    """
    Fetch videos from a YouTube playlist using pytubefix.
    Returns a list of video objects compatible with the scrapetube format.
    
    Args:
        playlist_id: The YouTube playlist ID
        limit: Maximum number of videos to fetch (None for all)
    
    Returns:
        List of video dictionaries with 'videoId' and 'title' keys
    """
    videos = []
    try:
        playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}"
        p = Playlist(playlist_url)
        
        video_urls = list(p.video_urls)
        if limit:
            video_urls = video_urls[:limit]
        
        for url in video_urls:
            # Extract video ID from URL
            if 'v=' in url:
                video_id = url.split('v=')[1].split('&')[0]
            else:
                video_id = url.split('/')[-1].split('?')[0]
            
            # Create video object compatible with scrapetube format
            videos.append({
                'videoId': video_id,
                'title': {'runs': [{'text': ''}]},  # Title will be fetched later during processing
                '_from_playlist': True,
                '_playlist_id': playlist_id
            })
    except Exception as e:
        print(f"      ⚠️ Error fetching playlist: {e}")
    
    return videos


def load_config():
    for config_file in CONFIG_FILES:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f: return json.load(f)
    return {}

def parse_published_time(published_text, max_days=1):
    """
    Parse YouTube's relative time text (e.g., '3 days ago', '1 week ago')
    Returns True if the video is within max_days, False otherwise.
    """
    if not published_text:
        return False
    text = published_text.lower().strip()
    
    # Handle "Streamed X ago" or "Premiered X ago"
    text = text.replace('streamed ', '').replace('premiered ', '')
    
    # Parse the time text
    try:
        if 'just now' in text or 'moment' in text:
            return True
        elif 'second' in text or 'minute' in text:
            return True
        elif 'hour' in text:
            return True
        elif 'day' in text:
            match = re.search(r'(\d+)\s*day', text)
            if match:
                days = int(match.group(1))
                return days <= max_days
            return max_days >= 1
        elif 'week' in text:
            match = re.search(r'(\d+)\s*week', text)
            if match:
                weeks = int(match.group(1))
                return (weeks * 7) <= max_days
            return max_days >= 7
        elif 'month' in text:
            match = re.search(r'(\d+)\s*month', text)
            if match:
                months = int(match.group(1))
                return (months * 30) <= max_days
            return max_days >= 30
        elif 'year' in text:
            return max_days >= 365
    except:
        pass
    return False

# --- TEXT CLEANING & NORMALIZATION ---

# Unicode fancy character mappings (mathematical italic, bold, etc. to ASCII)
UNICODE_TO_ASCII = {
    # Mathematical italic letters (U+1D44E - U+1D467)
    '𝐴': 'A', '𝐵': 'B', '𝐶': 'C', '𝐷': 'D', '𝐸': 'E', '𝐹': 'F', '𝐺': 'G', '𝐻': 'H', '𝐼': 'I',
    '𝐽': 'J', '𝐾': 'K', '𝐿': 'L', '𝑀': 'M', '𝑁': 'N', '𝑂': 'O', '𝑃': 'P', '𝑄': 'Q', '𝑅': 'R',
    '𝑆': 'S', '𝑇': 'T', '𝑈': 'U', '𝑉': 'V', '𝑊': 'W', '𝑋': 'X', '𝑌': 'Y', '𝑍': 'Z',
    '𝑎': 'a', '𝑏': 'b', '𝑐': 'c', '𝑑': 'd', '𝑒': 'e', '𝑓': 'f', '𝑔': 'g', '𝘩': 'h', '𝑖': 'i',
    '𝑗': 'j', '𝑘': 'k', '𝑙': 'l', '𝑚': 'm', '𝑛': 'n', '𝑜': 'o', '𝑝': 'p', '𝑞': 'q', '𝑟': 'r',
    '𝑠': 's', '𝑡': 't', '𝑢': 'u', '𝑣': 'v', '𝑤': 'w', '𝑥': 'x', '𝑦': 'y', '𝑧': 'z',
    # Mathematical bold letters
    '𝐀': 'A', '𝐁': 'B', '𝐂': 'C', '𝐃': 'D', '𝐄': 'E', '𝐅': 'F', '𝐆': 'G', '𝐇': 'H', '𝐈': 'I',
    '𝐉': 'J', '𝐊': 'K', '𝐋': 'L', '𝐌': 'M', '𝐍': 'N', '𝐎': 'O', '𝐏': 'P', '𝐐': 'Q', '𝐑': 'R',
    '𝐒': 'S', '𝐓': 'T', '𝐔': 'U', '𝐕': 'V', '𝐖': 'W', '𝐗': 'X', '𝐘': 'Y', '𝐙': 'Z',
    '𝐚': 'a', '𝐛': 'b', '𝐜': 'c', '𝐝': 'd', '𝐞': 'e', '𝐟': 'f', '𝐠': 'g', '𝐡': 'h', '𝐢': 'i',
    '𝐣': 'j', '𝐤': 'k', '𝐥': 'l', '𝐦': 'm', '𝐧': 'n', '𝐨': 'o', '𝐩': 'p', '𝐪': 'q', '𝐫': 'r',
    '𝐬': 's', '𝐭': 't', '𝐮': 'u', '𝐯': 'v', '𝐰': 'w', '𝐱': 'x', '𝐲': 'y', '𝐳': 'z',
    # Common decorative/fancy characters
    'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'ñ': 'n',
    'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U', 'Ñ': 'N',
    'ü': 'u', 'Ü': 'U', 'ö': 'o', 'Ö': 'O', 'ä': 'a', 'Ä': 'A',
    '–': '-', '—': '-', ''': "'", ''': "'", '"': '"', '"': '"',
    '…': '...', '•': '-', '·': '-',
}

# Maximum filename length in bytes (GitHub Pages limit)
MAX_FILENAME_BYTES = 255

def normalize_unicode_to_ascii(text):
    """
    Convert Unicode fancy characters (mathematical italic/bold, accents, etc.) to ASCII.
    This prevents filenames from being too long in bytes due to multi-byte Unicode chars.
    """
    # First, apply explicit mappings
    for unicode_char, ascii_char in UNICODE_TO_ASCII.items():
        text = text.replace(unicode_char, ascii_char)
    
    # Then, use unicodedata to normalize remaining characters
    import unicodedata
    # NFKD decomposition converts fancy chars to base + combining marks
    normalized = unicodedata.normalize('NFKD', text)
    # Keep only ASCII characters (removes combining marks)
    ascii_text = normalized.encode('ascii', 'ignore').decode('ascii')
    
    return ascii_text

def sanitize_filename(text, max_bytes=MAX_FILENAME_BYTES):
    """
    Sanitize text for use as a filename:
    1. Convert Unicode fancy characters to ASCII equivalents
    2. Remove illegal filename characters
    3. Collapse multiple spaces/dashes
    4. Truncate to max byte length while preserving word boundaries
    
    Args:
        text: The text to sanitize
        max_bytes: Maximum length in bytes (default 180 for GitHub Pages safety)
    
    Returns:
        A safe filename string
    """
    # Step 1: Convert Unicode fancy characters to ASCII
    text = normalize_unicode_to_ascii(text)
    
    # Step 2: Remove illegal filename characters
    text = re.sub(r'[\\/*?:"<>|#]', "", text)
    
    # Step 3: Collapse multiple spaces and dashes
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'-+', '-', text)
    text = text.strip(' -')
    
    # Step 4: Truncate if too long (accounting for .txt extension = 4 bytes)
    # Reserve space for date prefix "YYYY-MM-DD - " (13 bytes) and extension
    available_bytes = max_bytes - 4  # Reserve for .txt
    
    if len(text.encode('utf-8')) > available_bytes:
        # Truncate at word boundary
        while len(text.encode('utf-8')) > available_bytes and ' ' in text:
            text = text.rsplit(' ', 1)[0]
        # Final truncation if still too long
        while len(text.encode('utf-8')) > available_bytes:
            text = text[:-1]
        text = text.rstrip(' -')
    
    return text

def update_transcript_speaker_header(filepath, new_speaker):
    """
    Update the Speaker: line in the transcript file's internal header.
    This ensures the internal metadata matches the filename.
    
    CRITICAL: This must be called BEFORE renaming transcript files to prevent
    the mismatch bug where filename has correct speaker but internal header 
    still says "Unknown Speaker".
    
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
        # Silently fail - don't break the main process
        return False

def split_multiple_speakers(text):
    return [p.strip() for p in re.split(r'\s+&\s+|\s+and\s+|\s*/\s*|,\s*', text) if p.strip()]

def remove_dates_from_name(text):
    """Remove date patterns from text."""
    for pattern in DATE_PATTERNS:
        text = re.sub(pattern, ' ', text, flags=re.IGNORECASE)
    return re.sub(r'\s+', ' ', text).strip()

def normalize_speaker(speaker, title=""):
    """
    Normalize speaker name and filter out non-speakers.
    Returns empty string if the name should be filtered out.
    """
    if not speaker or speaker == "Unknown Speaker": 
        return speaker
    
    speaker = speaker.strip()
    s_lower = speaker.lower()
    
    # Specific fix maps - William Branham variations
    william_branham_patterns = [
        r'^William\s+Marrion\s+Branham$',
        r'^William\s+Marion\s+Branham$',
        r'^William\s+Branham$',
        r'^William\s+M\.?\s+Branham$',
        r'^Williams\s+Branham$',
        r'^Prophet\s+William\s+(?:M\.?\s+)?(?:Marrion\s+)?Branham$',
    ]
    for pattern in william_branham_patterns:
        if re.match(pattern, speaker, re.IGNORECASE):
            return "William M. Branham"
    
    if "prophet" in s_lower and "branham" in s_lower: return "William M. Branham"
    if "william" in s_lower and "branham" in s_lower: return "William M. Branham"
    if speaker.lower() == "branham": return "William M. Branham"
    
    # Other specific normalizations
    if "isaiah" in s_lower and "brooks" in s_lower: return "Isiah Brooks"
    if "caleb" in s_lower and "perez" in s_lower: return "Caleb Perez"
    if "daniel" in s_lower and "evans" in s_lower: return "Daniel Evans"
    if "andrew glover" in s_lower and "twt camp" in s_lower: return "Andrew Glover"
    if "andrew spencer" in s_lower and "july" in s_lower: return "Andrew Spencer"
    if "pr" in s_lower and "busobozi" in s_lower: return "Busobozi Talemwa"
    if "joel pruitt" in s_lower and "youth" in s_lower: return "Joel Pruitt"
    if re.match(r'^Dan\s+Evans$', speaker, re.IGNORECASE): return 'Daniel Evans'
    if re.match(r'^Faustin\s+Luk', speaker, re.IGNORECASE): return 'Faustin Lukumuena'

    # Choir Fixes
    if "choir" in s_lower:
        if "evening light" in s_lower: return "Evening Light Choir"
        if "bethel" in s_lower: return "Bethel Tabernacle Choir"
        return "Church Choir"
    
    # Filter out church/organization names
    church_patterns = [
        r'\b(?:Church|Tabernacle|Chapel|Temple|Ministry|Ministries|Fellowship)\b',
        r'\bInner\s+Veil\b', r'\bETM\s+Tab(?:ernacle)?\b', r'\bFGLT\b', r'\bBCF\b',
        r'\bCongregation\b', r'\bChoir\b', r'\bSouth\s+Africa\b', r'\bIvory\s+Coast\b',
    ]
    for pattern in church_patterns:
        if re.search(pattern, speaker, re.IGNORECASE):
            return ""
    
    # Filter out memorial service names (deceased, not speaker)
    if re.search(r'\bMemorial\b', speaker, re.IGNORECASE): return ""
    if title and re.search(r'\bMemorial\b', title, re.IGNORECASE):
        if 'billy paul' in speaker.lower(): return ""
    
    # Filter out topic phrases that look like names
    topic_patterns = [
        r'^Apostolic\s+Power\b', r'^Smoking\s+Habits\b', r'^Accepting\s+Trouble\b',
        r'^When\s+Jesus\b', r'^Prepare\s+Yourself\b', r'^Instrumental\b',
        r'\bBaptism$', r'\bPt\.?\s*\d*$', r'\bPart\s*\d*$', r'\bMusic$',
        r'^Song$', r'^Branham\s+Come\b', r'^Branham\s+Saw\b',
        r'^of\s+Christianity$', r'^Of\s+Malachi$', r'^What\s+Is\s+The\s+',
        r'^Reversal\s+Of\s+', r'^Perplexity\s+Of\s+', r'^Fit\s+To\s+Rule$',
        r'^At\s+Midnight$', r'^Atmosphere$', r'^Vaunteth\s+Not\b',
        r'^The\s+Appeal\b', r'^Character\s+With\b', r'^Battling\s+To\b',
        r'^\w+ing\s+The\s+', r'^\w+ed\s+By\s+',
    ]
    for pattern in topic_patterns:
        if re.search(pattern, speaker, re.IGNORECASE):
            return ""
    
    # Filter out non-person patterns
    non_person_patterns = [
        r'^Report$', r'^Worship$', r'^Prayer$', r'^Watch\s+Night\b',
        r'^\d+$', r'^Sisters$', r'^Sound\s+Of\s+Freedom$',
        r'^Faith\s+Is\s+Our\s+Victory$', r'^Friend\s+Of\s+God$',
        r'^Highway\s+To\s+Heaven$', r'^Goodness\s+Of\s+God$',
        r'^Shout\s+To\s+The\s+Lord$', r'^Through\s+The\s+Fire$',
        r'^As\s+The\s+Deer$', r'^Mark\s+Of\s+The\s+Beast$',
        r'^Marriage\s+And\s+Divorce$', r'^Look\s+To\s+Jesus$',
        r'^Congregational\s+Worship$', r'^Missionary\s+Report$',
    ]
    for pattern in non_person_patterns:
        if re.match(pattern, speaker, re.IGNORECASE):
            return ""
    
    # Clean up trailing topic words
    speaker = re.sub(r'\s+(?:Worship|Under|Redemption|At|Study|Pt|Part|Pastor|In)\s+.*$', '', speaker, flags=re.IGNORECASE)
    
    return speaker.strip()

def clean_name(name):
    """Clean up extracted name - improved version."""
    if not name:
        return ""
    
    # Remove leading/trailing punctuation and whitespace
    name = re.sub(r'^[\s\-:,;\.\'\"]+', '', name)
    name = re.sub(r'[\s\-:,;\.\'\"]+$', '', name)
    
    # Remove leading honorifics
    name = re.sub(r'^(?:By|Pr\.?|Br\.?|Bro\.?|Brother|Brothers|Bros\.?|Sister|Sis\.?|Sr\.?|Hna\.?|Hno\.?|Pastor|Ptr\.?|Pst\.?|Bishop|Rev\.?|Evangelist|Guest\s+Minister|Song\s+Leader|Elder|Founding)\s+', '', name, flags=re.IGNORECASE)
    
    # Remove dates - especially "on Month" patterns
    name = re.sub(r'\s+on\s+(?:January|February|March|April|May|June|July|August|September|October|November|December).*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+on\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?.*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d.*$', '', name, flags=re.IGNORECASE)
    
    # Remove trailing abbreviated months
    name = re.sub(r'\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?$', '', name, flags=re.IGNORECASE)
    
    # Remove other dates
    name = remove_dates_from_name(name)
    
    # Remove part numbers
    name = re.sub(r'\s*\(?(?:Part|Pt\.?)\s*\d+\)?.*$', '', name, flags=re.IGNORECASE)
    
    # Remove trailing date words
    name = re.sub(r'\s+(?:on|at|from|during)\s*$', '', name, flags=re.IGNORECASE)
    
    # Remove location info like "at Church Name"
    name = re.sub(r'\s+at\s+[A-Z].*$', '', name)
    
    # Clean up quotes
    name = re.sub(r'["\'"]+', '', name)
    
    # Remove trailing "and Congregation", etc.
    name = re.sub(r'\s+and\s+(?:Congregation|Saints|Sis\.?|Bro\.?|Sister|Brother).*$', '', name, flags=re.IGNORECASE)
    
    # Remove trailing time/service words
    name = re.sub(r'\s+(?:Morning|Evening|Afternoon|Night|Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday)\s*(?:Service|Night|Morning|Evening|Communion)?.*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+(?:Service|Worship|Meeting|Night|Communion)$', '', name, flags=re.IGNORECASE)
    
    # Remove trailing "Eve" and "Instrumental"
    name = re.sub(r'\s+Eve$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+Instrumental$', '', name, flags=re.IGNORECASE)
    
    # Remove trailing sermon-related suffixes
    name = re.sub(r'\s+(?:Minister\s+Meeting|Youth\s+Service|Youth|Timestamps|Esther|Part\s*\d*)$', '', name, flags=re.IGNORECASE)
    
    # Remove trailing numbers like "1 of 2"
    name = re.sub(r'\s*[\[\(]?\d+\s+of\s+\d+[\]\)]?$', '', name, flags=re.IGNORECASE)
    
    # Remove ALL CAPS suffix (likely sermon titles)
    name = re.sub(r'\s+[A-Z]{2,}(?:\s+[A-Z]{2,})*$', '', name)
    
    # Remove trailing junk words
    name = name.strip(" .,:;-|")
    words = name.split()
    while words and words[-1].lower() in INVALID_NAME_TERMS:
        words.pop()
    name = " ".join(words)
    
    # Clean multiple spaces
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name

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

def is_valid_person_name(text, title=""):
    """Check if text looks like a valid person name - improved version."""
    if not text or not text.strip():
        return False
    
    text = text.strip()
    t_lower = text.lower()
    
    # Add specific exceptions for valid names that might otherwise fail
    valid_exceptions = ["church choir", "bloteh won", "chris take", "tim cross", 
                       "william m. branham", "isiah brooks", "daniel evans", "caleb perez"]
    if t_lower in valid_exceptions:
        return True
    
    # Check against NON_NAME_PATTERNS
    for pattern in NON_NAME_PATTERNS:
        if re.match(pattern, text, re.IGNORECASE):
            return False
    
    # Reject obvious junk
    if t_lower.startswith(("the ", "a ", "an ", "i ", "my ", "if ", "this ", "that ", "when ", "where ", "what ", "how ", "why ")): 
        return False
    if t_lower.endswith((" the", " a", " is", " are", " was", " be")):
        return False
    
    # Reject service/content words
    service_words = ["hymn", "service", "sermon", "worship", "meeting", "prayer", "song", 
                     "baptism", "dedication", "funeral", "memorial", "testimony", "testimonies",
                     "communion", "supper", "revival", "conference", "camp"]
    for word in service_words:
        if word in t_lower:
            return False
    
    # Reject topic keywords
    topic_keywords = ["how", "why", "when", "where", "what", "should", "must", "will",
                      "shall", "being", "having", "taking", "making", "getting", "going"]
    for word in topic_keywords:
        if t_lower.startswith(word + " ") or t_lower.endswith(" " + word):
            return False
    
    # Check invalid terms
    text_words = t_lower.split()
    for word in text_words:
        w_clean = word.strip(".,:;-")
        if w_clean in INVALID_NAME_TERMS:
            return False
    
    # Must have reasonable word count (2-5 words typical for names)
    words = text.split()
    if not (2 <= len(words) <= 5):
        return False
    
    # Capitalization Check
    allowed_lowercase = {'de', 'la', 'van', 'der', 'st', 'mc', 'mac', 'del', 'dos', 'da', 'von', 'di', 'le', 'du', 'al', 'el'}
    for w in words:
        clean_w = w.replace('.', '').replace("'", "").replace("-", "")
        # Allow names with apostrophes and hyphens
        if not clean_w.isalpha(): 
            # Check if it's just punctuation issues
            if re.match(r'^[A-Za-z\'\-\.]+$', w):
                continue
            return False
        if not w[0].isupper():
            if w.lower() not in allowed_lowercase: 
                return False
    
    # Use spaCy if available for entity checking
    if nlp:
        doc = nlp(text)
        for ent in doc.ents:
            if ent.label_ in ["ORG", "DATE", "TIME", "GPE", "PRODUCT", "FAC", "EVENT", "LAW", "WORK_OF_ART"]: 
                return False
    
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
    
    # Load known speakers for full speaker detection
    known_speakers = load_json_file(SPEAKERS_FILE)
    
    # Statistics tracking for speaker detection
    heal_stats = {
        'total_processed': 0,
        'speakers_detected': 0,
        'unknown_speakers': 0,
        'speakers_redetected': 0,
        'speakers_corrected': 0,
        'new_speakers': set(),
        'by_church': {}
    }
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
        
        # Initialize per-church stats
        church_stats = {'total': 0, 'detected': 0, 'unknown': 0}
        
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
            
            # Track stats
            church_stats['total'] += 1
            heal_stats['total_processed'] += 1
            
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

            # --- STEP 1.5: FULL SPEAKER DETECTION FOR UNKNOWN SPEAKERS ---
            # If speaker is still unknown after smart correction, run full detection algorithm
            if new_speaker == "Unknown Speaker" or not new_speaker:
                description = row.get('description', '')
                detected_speaker, is_new = identify_speaker_dynamic(original_title, description, known_speakers)
                detected_speaker = normalize_speaker(detected_speaker)
                detected_speaker = clean_name(detected_speaker)
                
                if detected_speaker and detected_speaker != "Unknown Speaker":
                    new_speaker = detected_speaker
                    heal_stats['speakers_redetected'] += 1
                    print(f"      🔍 DETECTED: '{original_title[:40]}...' -> {new_speaker}")
                    if is_new:
                        heal_stats['new_speakers'].add(new_speaker)
                        known_speakers.add(new_speaker)
                        print(f"      🎉 NEW SPEAKER LEARNED: {new_speaker}")

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
                # Track stats for unchanged rows too
                if new_speaker and new_speaker != "Unknown Speaker":
                    church_stats['detected'] += 1
                    heal_stats['speakers_detected'] += 1
                else:
                    church_stats['unknown'] += 1
                    heal_stats['unknown_speakers'] += 1
                continue
                
            # SOMETHING CHANGED -> UPDATE FILE & DB
            change_detected = False
            if new_speaker != original_speaker:
                print(f"      - Speaker Change: '{original_speaker}' -> '{new_speaker}'")
                change_detected = True
                heal_stats['speakers_corrected'] += 1
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
            
            # Track final speaker status for stats
            if canonical_speaker and canonical_speaker != "Unknown Speaker":
                church_stats['detected'] += 1
                heal_stats['speakers_detected'] += 1
            else:
                church_stats['unknown'] += 1
                heal_stats['unknown_speakers'] += 1
            
        # Save church stats if any rows processed
        if church_stats['total'] > 0:
            heal_stats['by_church'][church_folder.replace('_', ' ')] = church_stats
            
        # Write back updated Summary CSV (with deduplication)
        # ARCHIVAL RULE: Use URL as primary key when available to avoid losing distinct videos
        try:
            seen_keys = set()
            deduped_rows = []
            for row in new_rows:
                url = row.get('url', '').strip()
                if url:
                    # Use URL as unique key (most reliable)
                    key = url
                else:
                    # Fallback to (date, title, speaker) for entries without URL
                    key = (row.get('date', '').strip(), row.get('title', '').strip(), row.get('speaker', '').strip())
                if key not in seen_keys:
                    seen_keys.add(key)
                    deduped_rows.append(row)
            with open(summary_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(deduped_rows)
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
    print(f"🔍 Speakers Re-detected: {heal_stats['speakers_redetected']}")
    
    # Write speaker detection log
    write_speaker_detection_log(heal_stats, operation_name="Heal Archive")

def heal_speakers_from_csv(csv_path="data/master_sermons_with_speaker_detected.csv"):
    """
    Heals speaker names in transcript files and summary CSVs based on a CSV file
    with corrected speakers in the 'speaker_detected' column.
    
    The input CSV should have at minimum these columns:
    - videoUrl: YouTube URL to identify the sermon
    - speaker_detected: The corrected speaker name to apply
    - church: Church name (to find the right folder/summary)
    - title: Sermon title (for matching)
    - date: Sermon date (for matching)
    
    This function:
    1. Reads the CSV with speaker_detected corrections
    2. For each entry where speaker_detected differs from current speaker:
       - Updates the transcript .txt file (header + filename)
       - Updates the Summary CSV entry
    3. Never deletes any entries - only modifies speaker field
    """
    print("\n" + "="*60)
    print("🔧 SPEAKER HEALING FROM CSV")
    print(f"   Source file: {csv_path}")
    print("="*60)
    
    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        return
    
    # Load the corrections CSV
    corrections = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Only include rows where speaker_detected is provided and non-empty
                speaker_detected = row.get('speaker_detected', '').strip()
                if speaker_detected:
                    corrections.append(row)
        print(f"   📋 Loaded {len(corrections)} rows with speaker_detected values")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return
    
    if not corrections:
        print("   ⚠️ No corrections found in CSV")
        return
    
    # Group corrections by church for efficient processing
    by_church = {}
    for row in corrections:
        church = row.get('church', '').strip()
        if church:
            if church not in by_church:
                by_church[church] = []
            by_church[church].append(row)
    
    updated_transcripts = 0
    updated_summaries = 0
    skipped_same = 0
    skipped_not_found = 0
    
    # Process each church
    for church_name, church_corrections in by_church.items():
        church_folder = church_name.replace(' ', '_')
        church_dir = os.path.join(DATA_DIR, church_folder)
        summary_path = os.path.join(DATA_DIR, f"{church_folder}_Summary.csv")
        
        print(f"\n   🏥 Processing: {church_name} ({len(church_corrections)} corrections)")
        
        # Load existing summary CSV
        summary_rows = []
        summary_by_url = {}
        if os.path.exists(summary_path):
            try:
                with open(summary_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        summary_rows.append(row)
                        url = row.get('url', '')
                        if url:
                            summary_by_url[url] = row
            except Exception as e:
                print(f"      ⚠️ Error reading summary: {e}")
        
        summary_modified = False
        
        for correction in church_corrections:
            video_url = correction.get('videoUrl', '').strip()
            new_speaker = correction.get('speaker_detected', '').strip()
            old_speaker = correction.get('speaker', '').strip()
            title = correction.get('title', '').strip()
            date = correction.get('date', '').strip()
            video_type = correction.get('type', 'Full Sermon').strip()
            language = correction.get('language', 'English').strip()
            
            # Skip if speaker_detected is same as current speaker
            if new_speaker.lower() == old_speaker.lower():
                skipped_same += 1
                continue
            
            # Normalize the new speaker
            new_speaker = normalize_speaker(new_speaker)
            new_speaker = clean_name(new_speaker)
            
            if not new_speaker or new_speaker == "Unknown Speaker":
                continue
            
            # --- Update Summary CSV ---
            if video_url in summary_by_url:
                summary_entry = summary_by_url[video_url]
                if summary_entry.get('speaker') != new_speaker:
                    summary_entry['speaker'] = new_speaker
                    summary_modified = True
                    updated_summaries += 1
            
            # --- Update Transcript File ---
            if os.path.isdir(church_dir):
                # Build old filename pattern
                safe_title = sanitize_filename(title)
                safe_old_speaker = sanitize_filename(old_speaker)
                safe_new_speaker = sanitize_filename(new_speaker)
                
                old_filename = f"{date} - {safe_title} - {safe_old_speaker}.txt"
                new_filename = f"{date} - {safe_title} - {safe_new_speaker}.txt"
                old_filepath = os.path.join(church_dir, old_filename)
                new_filepath = os.path.join(church_dir, new_filename)
                
                if os.path.exists(old_filepath):
                    try:
                        with open(old_filepath, 'r', encoding='utf-8') as f:
                            content = f.read()
                        
                        # Update speaker in header
                        content = re.sub(r'Speaker:.*', f'Speaker: {new_speaker}', content)
                        content = re.sub(r'START OF FILE:.*', f'START OF FILE: {new_filename}', content)
                        
                        # Write to new filename
                        with open(new_filepath, 'w', encoding='utf-8') as f:
                            f.write(content)
                        
                        # Remove old file if different
                        if old_filepath != new_filepath:
                            os.remove(old_filepath)
                        
                        updated_transcripts += 1
                        print(f"      ✅ {old_speaker} → {new_speaker}: {title[:40]}...")
                    except Exception as e:
                        print(f"      ⚠️ Error updating transcript: {e}")
                else:
                    # Try to find file by other means (partial match)
                    found = False
                    for txt_file in os.listdir(church_dir):
                        if txt_file.endswith('.txt') and date in txt_file and safe_title[:20] in txt_file:
                            old_filepath = os.path.join(church_dir, txt_file)
                            try:
                                with open(old_filepath, 'r', encoding='utf-8') as f:
                                    content = f.read()
                                content = re.sub(r'Speaker:.*', f'Speaker: {new_speaker}', content)
                                content = re.sub(r'START OF FILE:.*', f'START OF FILE: {new_filename}', content)
                                with open(new_filepath, 'w', encoding='utf-8') as f:
                                    f.write(content)
                                if old_filepath != new_filepath:
                                    os.remove(old_filepath)
                                updated_transcripts += 1
                                found = True
                                print(f"      ✅ {old_speaker} → {new_speaker}: {title[:40]}...")
                                break
                            except:
                                pass
                    if not found:
                        skipped_not_found += 1
        
        # Write updated summary CSV
        if summary_modified and summary_rows:
            try:
                fieldnames = ["date", "status", "speaker", "title", "url", "last_checked", "language", "type", "description", "duration", "church"]
                with open(summary_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(summary_rows)
                print(f"      📝 Summary CSV updated")
            except Exception as e:
                print(f"      ⚠️ Error writing summary: {e}")
    
    # Update speakers.json with new speakers
    known_speakers = load_json_file(SPEAKERS_FILE)
    new_speakers_added = set()
    for correction in corrections:
        new_speaker = correction.get('speaker_detected', '').strip()
        if new_speaker and new_speaker != "Unknown Speaker":
            new_speaker = normalize_speaker(new_speaker)
            new_speaker = clean_name(new_speaker)
            if new_speaker and is_valid_person_name(new_speaker):
                if new_speaker not in known_speakers:
                    new_speakers_added.add(new_speaker)
                known_speakers.add(new_speaker)
    save_json_file(SPEAKERS_FILE, known_speakers)
    
    print("\n" + "="*60)
    print("📊 SPEAKER HEALING SUMMARY")
    print(f"   ✅ Transcripts updated: {updated_transcripts}")
    print(f"   ✅ Summary entries updated: {updated_summaries}")
    print(f"   ⏭️ Skipped (same speaker): {skipped_same}")
    print(f"   ⚠️ Skipped (file not found): {skipped_not_found}")
    print("="*60)
    
    # Write statistics to log file
    total_processed = len(corrections)
    speakers_detected = updated_transcripts + updated_summaries + skipped_same
    unknown_speakers = skipped_not_found
    
    stats = {
        'total_processed': total_processed,
        'speakers_detected': speakers_detected,
        'unknown_speakers': unknown_speakers,
        'transcripts_updated': updated_transcripts,
        'summaries_updated': updated_summaries,
        'skipped_same': skipped_same,
        'skipped_not_found': skipped_not_found,
        'new_speakers': new_speakers_added,
    }
    write_speaker_detection_log(stats, operation_name="Heal Speakers from CSV")


def backfill_descriptions(data_dir=None, dry_run=False, churches=None, limit=None):
    """
    Backfill video descriptions into existing transcript files that are missing them.
    
    This function:
    1. Scans all transcript .txt files in data_dir
    2. Checks if each file has a Description section
    3. If missing, extracts the video ID from the URL in the file
    4. Fetches the description from YouTube
    5. Updates the file with the description section
    
    Args:
        data_dir: Path to data directory (defaults to DATA_DIR)
        dry_run: If True, only report what would be done without making changes
        churches: List of church folder names to process (None = all churches)
        limit: Maximum number of files to process (None = no limit)
    
    Returns:
        Tuple of (files_updated, files_skipped, files_with_errors)
    """
    if data_dir is None:
        data_dir = DATA_DIR
    
    print("\n" + "="*60)
    print("📝 BACKFILLING VIDEO DESCRIPTIONS INTO TRANSCRIPT FILES")
    if dry_run:
        print("   ⚠️ DRY RUN MODE - No files will be modified")
    if churches:
        print(f"   🏛️ Churches filter: {', '.join(churches)}")
    if limit:
        print(f"   📊 Limit: {limit} files max")
    print("="*60)
    
    files_updated = 0
    files_skipped = 0
    files_already_have_desc = 0
    files_with_errors = 0
    files_no_transcript = 0
    limit_reached = False
    
    # Get list of church folders to process
    all_church_folders = sorted(os.listdir(data_dir))
    
    # Filter by specified churches if provided
    if churches:
        # Normalize church names for matching (handle underscores/spaces)
        normalized_churches = []
        for c in churches:
            normalized_churches.append(c)
            normalized_churches.append(c.replace(' ', '_'))
            normalized_churches.append(c.replace('_', ' '))
        
        church_folders = [f for f in all_church_folders 
                         if f in normalized_churches or 
                         any(c.lower() in f.lower() for c in churches)]
    else:
        church_folders = all_church_folders
    
    # Iterate through church folders
    for church_folder in church_folders:
        if limit_reached:
            break
            
        church_path = os.path.join(data_dir, church_folder)
        if not os.path.isdir(church_path):
            continue
        if church_folder.startswith('.') or church_folder.endswith('.csv'):
            continue
        
        print(f"\n📂 Processing: {church_folder}")
        church_updated = 0
        church_skipped = 0
        
        for filename in sorted(os.listdir(church_path)):
            # Check if we've hit the limit
            if limit and files_updated >= limit:
                print(f"\n⚠️ Limit of {limit} files reached. Stopping.")
                limit_reached = True
                break
                
            if not filename.endswith('.txt'):
                continue
            
            filepath = os.path.join(church_path, filename)
            
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Check if file already has a Description section
                if 'Description:' in content and content.find('Description:') < content.find('TRANSCRIPT'):
                    files_already_have_desc += 1
                    continue
                
                # Extract video ID from URL in file
                url_match = re.search(r'URL:\s*(https?://[^\s]+)', content)
                if not url_match:
                    files_skipped += 1
                    church_skipped += 1
                    continue
                
                url = url_match.group(1).strip()
                video_id = None
                if 'v=' in url:
                    video_id = url.split('v=')[1].split('&')[0]
                elif 'youtu.be/' in url:
                    video_id = url.split('/')[-1].split('?')[0]
                
                if not video_id:
                    files_skipped += 1
                    church_skipped += 1
                    continue
                
                if dry_run:
                    print(f"   Would fetch description for: {filename[:60]}...")
                    files_updated += 1
                    church_updated += 1
                    continue
                
                # Fetch description from YouTube
                try:
                    _, description, yt_obj, _ = get_transcript_data(video_id)
                except Exception as e:
                    print(f"   ⚠️ Error fetching {filename[:40]}: {e}")
                    files_with_errors += 1
                    continue
                
                if not description or not description.strip():
                    files_no_transcript += 1
                    continue
                
                # Truncate description if too long
                desc_text = description[:2000] + "..." if len(description) > 2000 else description
                desc_section = f"Description:\n{desc_text}\n\n"
                
                # Insert description section before TRANSCRIPT section
                if 'TRANSCRIPT' in content:
                    new_content = content.replace(
                        'TRANSCRIPT\n========================================',
                        f'{desc_section}TRANSCRIPT\n========================================'
                    )
                else:
                    # Fallback: insert after the header block
                    new_content = re.sub(
                        r'(========================================\n\n)',
                        f'\\1{desc_section}',
                        content,
                        count=1
                    )
                
                # Write updated content
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                
                files_updated += 1
                church_updated += 1
                print(f"   ✅ Added description to: {filename[:50]}...")
                
            except Exception as e:
                print(f"   ❌ Error processing {filename}: {e}")
                files_with_errors += 1
        
        if church_updated > 0:
            print(f"   📊 Church summary: {church_updated} updated, {church_skipped} skipped")
    
    print("\n" + "="*60)
    print("📊 DESCRIPTION BACKFILL SUMMARY")
    print(f"   ✅ Files updated: {files_updated}")
    print(f"   ✓ Already had description: {files_already_have_desc}")
    print(f"   ⏭️ Skipped (no URL/video ID): {files_skipped}")
    print(f"   📭 No description available: {files_no_transcript}")
    print(f"   ❌ Errors: {files_with_errors}")
    print("="*60)
    
    return files_updated, files_skipped, files_with_errors


def add_timestamps_for_video(video_url_or_id, data_dir=None):
    """
    Add timestamped transcript for a specific video that has already been scraped.
    
    This function:
    1. Extracts the video ID from the URL
    2. Searches for the existing transcript file in data_dir
    3. Fetches timestamped captions from YouTube
    4. Creates the .timestamped.txt file
    
    Args:
        video_url_or_id: YouTube URL or video ID (e.g., "dQw4w9WgXcQ" or "https://www.youtube.com/watch?v=dQw4w9WgXcQ")
        data_dir: Path to data directory (defaults to DATA_DIR)
    
    Returns:
        True if successful, False otherwise
    """
    if data_dir is None:
        data_dir = DATA_DIR
    
    # Extract video ID from URL or use as-is
    video_id = video_url_or_id
    if 'youtube.com' in video_url_or_id or 'youtu.be' in video_url_or_id:
        # Extract video ID from various URL formats
        match = re.search(r'(?:v=|youtu\.be/)([a-zA-Z0-9_-]+)', video_url_or_id)
        if match:
            video_id = match.group(1)
        else:
            print(f"❌ Could not extract video ID from: {video_url_or_id}")
            return False
    
    print("\n" + "="*60)
    print("📍 ADDING TIMESTAMPS FOR SPECIFIC VIDEO")
    print(f"   🎬 Video ID: {video_id}")
    print("="*60)
    
    # Search for the existing transcript file
    found_file = None
    found_church = None
    
    for church_folder in os.listdir(data_dir):
        church_path = os.path.join(data_dir, church_folder)
        if not os.path.isdir(church_path):
            continue
        if church_folder.startswith('.') or church_folder.endswith('.csv'):
            continue
        
        for filename in os.listdir(church_path):
            if not filename.endswith('.txt') or filename.endswith('.timestamped.txt'):
                continue
            
            filepath = os.path.join(church_path, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read(2000)  # Check header only
                
                if video_id in content:
                    found_file = filepath
                    found_church = church_folder
                    break
            except:
                continue
        
        if found_file:
            break
    
    if not found_file:
        print(f"❌ No existing transcript found for video ID: {video_id}")
        print("   This video may not have been scraped yet, or the video ID is incorrect.")
        return False
    
    print(f"   📂 Found in: {found_church}")
    print(f"   📄 File: {os.path.basename(found_file)}")
    
    # Check if timestamps already exist
    timestamped_path = found_file.replace('.txt', '.timestamped.txt')
    if os.path.exists(timestamped_path):
        print(f"   ⚠️ Timestamped file already exists: {os.path.basename(timestamped_path)}")
        response = input("   Do you want to overwrite it? (y/N): ").strip().lower()
        if response != 'y':
            print("   ⏭️ Skipped.")
            return False
    
    # Fetch timestamps from YouTube
    print(f"   🔄 Fetching timestamps from YouTube...")
    try:
        time.sleep(1)  # Rate limiting
        _, _, _, raw_xml = get_transcript_data(video_id)
        
        if not raw_xml:
            print(f"   ❌ No captions available for this video.")
            return False
        
        # Parse timestamps and save
        segments = xml_to_timestamped_segments(raw_xml)
        if segments:
            if save_timestamp_data(found_file, video_id, segments):
                print(f"   ✅ Created timestamped transcript ({len(segments)} segments)")
                print(f"   📄 Saved to: {os.path.basename(timestamped_path)}")
                return True
            else:
                print(f"   ❌ Failed to save timestamps")
                return False
        else:
            print(f"   ❌ No segments parsed from captions")
            return False
            
    except Exception as e:
        print(f"   ❌ Error fetching timestamps: {e}")
        return False


def backfill_timestamps(data_dir=None, dry_run=False, churches=None, limit=None):
    """
    Backfill timestamped transcript data for existing .txt files that are missing .timestamps.json.
    
    This function:
    1. Scans all transcript .txt files in data_dir
    2. Checks if each file has a corresponding .timestamps.json
    3. If missing, extracts the video ID from the URL in the file
    4. Fetches the caption XML from YouTube
    5. Parses timestamps and creates the .timestamps.json file
    
    Args:
        data_dir: Path to data directory (defaults to DATA_DIR)
        dry_run: If True, only report what would be done without making changes
        churches: List of church folder names to process (None = all churches)
        limit: Maximum number of files to process (None = no limit)
    
    Returns:
        Tuple of (files_updated, files_skipped, files_with_errors)
    """
    if data_dir is None:
        data_dir = DATA_DIR
    
    print("\n" + "="*60)
    print("📍 BACKFILLING TIMESTAMPS FOR TRANSCRIPT FILES")
    if dry_run:
        print("   ⚠️ DRY RUN MODE - No files will be created")
    if churches:
        print(f"   🏛️ Churches filter: {', '.join(churches)}")
    if limit:
        print(f"   📊 Limit: {limit} files max")
    print("="*60)
    
    files_created = 0
    files_skipped = 0
    files_already_have_timestamps = 0
    files_with_errors = 0
    files_no_captions = 0
    limit_reached = False
    
    # Get list of church folders to process
    all_church_folders = sorted(os.listdir(data_dir))
    
    # Filter by specified churches if provided
    if churches:
        # Normalize church names for matching (handle underscores/spaces)
        normalized_churches = []
        for c in churches:
            normalized_churches.append(c)
            normalized_churches.append(c.replace(' ', '_'))
            normalized_churches.append(c.replace('_', ' '))
        
        church_folders = [f for f in all_church_folders 
                         if f in normalized_churches or 
                         any(c.lower() in f.lower() for c in churches)]
    else:
        church_folders = all_church_folders
    
    # Count total .txt files without .timestamps.json for progress
    total_to_process = 0
    for church_folder in church_folders:
        church_path = os.path.join(data_dir, church_folder)
        if not os.path.isdir(church_path):
            continue
        if church_folder.startswith('.') or church_folder.endswith('.csv'):
            continue
        for filename in os.listdir(church_path):
            if filename.endswith('.txt'):
                json_path = os.path.join(church_path, filename.replace('.txt', '.timestamps.json'))
                if not os.path.exists(json_path):
                    total_to_process += 1
    
    print(f"   📄 Found {total_to_process} transcript files without timestamps")
    
    processed = 0
    
    # Iterate through church folders
    for church_folder in church_folders:
        if limit_reached:
            break
            
        church_path = os.path.join(data_dir, church_folder)
        if not os.path.isdir(church_path):
            continue
        if church_folder.startswith('.') or church_folder.endswith('.csv'):
            continue
        
        print(f"\n📂 Processing: {church_folder}")
        church_created = 0
        church_skipped = 0
        
        for filename in sorted(os.listdir(church_path)):
            # Check if we've hit the limit
            if limit and files_created >= limit:
                print(f"\n⚠️ Limit of {limit} files reached. Stopping.")
                limit_reached = True
                break
                
            if not filename.endswith('.txt') or filename.endswith('.timestamped.txt'):
                continue
            
            filepath = os.path.join(church_path, filename)
            timestamped_path = filepath.replace('.txt', '.timestamped.txt')
            
            # Skip if timestamped transcript already exists
            if os.path.exists(timestamped_path):
                files_already_have_timestamps += 1
                continue
            
            processed += 1
            
            # Read the file to extract video ID from URL
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read(2000)  # Only need the header
            except Exception as e:
                print(f"   ⚠️ Error reading {filename}: {e}")
                files_with_errors += 1
                continue
            
            # Extract video ID from URL line
            url_match = re.search(r'URL:\s*https://www\.youtube\.com/watch\?v=([a-zA-Z0-9_-]+)', content)
            if not url_match:
                files_skipped += 1
                church_skipped += 1
                continue
            
            video_id = url_match.group(1)
            
            if dry_run:
                print(f"   [{processed}/{total_to_process}] Would fetch timestamps for: {filename[:50]}...")
                files_created += 1
                church_created += 1
                continue
            
            print(f"   [{processed}/{total_to_process}] Fetching timestamps for: {filename[:50]}...")
            
            # Fetch captions from YouTube
            try:
                time.sleep(1)  # Rate limiting
                _, _, _, raw_xml = get_transcript_data(video_id)
                
                if not raw_xml:
                    files_no_captions += 1
                    print(f"      ⏭️ No captions available")
                    continue
                
                # Parse timestamps and save
                segments = xml_to_timestamped_segments(raw_xml)
                if segments:
                    if save_timestamp_data(filepath, video_id, segments):
                        files_created += 1
                        church_created += 1
                        print(f"      ✅ Created timestamps ({len(segments)} segments)")
                    else:
                        files_with_errors += 1
                        print(f"      ❌ Failed to save timestamps")
                else:
                    files_no_captions += 1
                    print(f"      ⏭️ No segments parsed")
                    
            except Exception as e:
                print(f"      ❌ Error: {str(e)[:50]}")
                files_with_errors += 1
        
        if church_created > 0:
            print(f"   📊 Church summary: {church_created} created, {church_skipped} skipped")
    
    print("\n" + "="*60)
    print("📊 TIMESTAMP BACKFILL SUMMARY")
    print(f"   ✅ Timestamp files created: {files_created}")
    print(f"   ✓ Already had timestamps: {files_already_have_timestamps}")
    print(f"   ⏭️ Skipped (no URL/video ID): {files_skipped}")
    print(f"   📭 No captions available: {files_no_captions}")
    print(f"   ❌ Errors: {files_with_errors}")
    print("="*60)
    
    return files_created, files_skipped, files_with_errors


def migrate_csv_add_church_names(data_dir):
    """
    Quick migration to add church name column to all existing CSV summary files.
    The church name is derived from the CSV filename.
    """
    print("\n" + "="*60)
    print("🏛️ MIGRATING CSV FILES: Adding Church Names")
    print("="*60)
    
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('_Summary.csv')]
    updated_count = 0
    skipped_count = 0
    
    for csv_file in sorted(csv_files):
        csv_path = os.path.join(data_dir, csv_file)
        
        # Extract church name from filename (e.g., "Arizona_Believers_Church_Summary.csv" -> "Arizona Believers Church")
        church_name = csv_file.replace('_Summary.csv', '').replace('_', ' ')
        
        try:
            # Read existing CSV
            rows = []
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                original_fieldnames = reader.fieldnames or []
                for row in reader:
                    # Add church name if not present or empty
                    if 'church' not in row or not row.get('church'):
                        row['church'] = church_name
                    rows.append(row)
            
            # Define complete fieldnames including new columns
            fieldnames = ["date", "status", "speaker", "title", "url", "last_checked", "language", "type", "description", "duration", "church"]
            
            # Write updated CSV
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(rows)
            
            print(f"   ✅ {church_name}: {len(rows)} entries updated")
            updated_count += 1
            
        except Exception as e:
            print(f"   ❌ Error processing {csv_file}: {e}")
            skipped_count += 1
    
    print("\n" + "="*60)
    print("📊 MIGRATION SUMMARY")
    print(f"   ✅ CSV files updated: {updated_count}")
    print(f"   ❌ Errors: {skipped_count}")
    print("="*60)
    
    return updated_count, skipped_count


def backfill_duration_metadata(data_dir, dry_run=False, churches=None, limit=None):
    """
    Scrape duration metadata for videos that are missing it in CSV summary files.
    """
    print("\n" + "="*60)
    print("⏱️ BACKFILLING VIDEO DURATION METADATA")
    if dry_run:
        print("   🔍 DRY RUN MODE - No changes will be made")
    if churches:
        print(f"   📂 Filtering to churches: {churches}")
    if limit:
        print(f"   🔢 Limit: {limit} videos per church")
    print("="*60)
    
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('_Summary.csv')]
    
    total_updated = 0
    total_skipped = 0
    total_errors = 0
    
    for csv_file in sorted(csv_files):
        church_name = csv_file.replace('_Summary.csv', '').replace('_', ' ')
        
        # Filter by church if specified
        if churches:
            church_list = [c.strip().lower() for c in churches.split(',')]
            if not any(c in church_name.lower() for c in church_list):
                continue
        
        csv_path = os.path.join(data_dir, csv_file)
        print(f"\n📂 Processing: {church_name}")
        
        try:
            # Read existing CSV
            rows = []
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            # Find entries missing duration
            missing_duration = []
            for i, row in enumerate(rows):
                duration = row.get('duration', '')
                if not duration or duration == '0' or duration == '':
                    url = row.get('url', '')
                    if url and 'watch?v=' in url:
                        missing_duration.append((i, row))
            
            if not missing_duration:
                print(f"   ✓ All {len(rows)} entries have duration")
                total_skipped += len(rows)
                continue
            
            print(f"   📊 Found {len(missing_duration)} entries missing duration")
            
            # Apply limit
            to_process = missing_duration[:limit] if limit else missing_duration
            
            updated_in_church = 0
            for idx, (row_idx, row) in enumerate(to_process, 1):
                url = row.get('url', '')
                video_id = url.split('watch?v=')[-1].split('&')[0]
                title = row.get('title', '')[:40]
                
                if dry_run:
                    print(f"   [{idx}/{len(to_process)}] Would fetch duration for: {title}...")
                    updated_in_church += 1
                    continue
                
                print(f"   [{idx}/{len(to_process)}] Fetching: {title}...")
                
                try:
                    time.sleep(1)  # Rate limiting
                    yt_obj = YouTube(url, use_oauth=False, allow_oauth_cache=True)
                    duration_seconds = yt_obj.length if hasattr(yt_obj, 'length') else 0
                    duration_minutes = int(duration_seconds / 60) if duration_seconds else 0
                    
                    rows[row_idx]['duration'] = duration_minutes
                    print(f"      ✅ Duration: {duration_minutes}m")
                    updated_in_church += 1
                    
                except Exception as e:
                    print(f"      ❌ Error: {str(e)[:40]}")
                    total_errors += 1
            
            # Write updated CSV
            if not dry_run and updated_in_church > 0:
                fieldnames = ["date", "status", "speaker", "title", "url", "last_checked", "language", "type", "description", "duration", "church"]
                with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(rows)
                print(f"   📝 Saved {updated_in_church} duration updates")
            
            total_updated += updated_in_church
            
        except Exception as e:
            print(f"   ❌ Error processing {csv_file}: {e}")
            total_errors += 1
    
    print("\n" + "="*60)
    print("📊 DURATION BACKFILL SUMMARY")
    print(f"   ✅ Durations updated: {total_updated}")
    print(f"   ✓ Skipped (already have duration): {total_skipped}")
    print(f"   ❌ Errors: {total_errors}")
    print("="*60)
    
    return total_updated, total_skipped, total_errors


def heal_video_categories(data_dir, dry_run=False, churches=None):
    """
    Post-scrape healing function to re-evaluate and fix video type categories.
    Uses title, description, and duration to determine correct category.
    """
    print("\n" + "="*60)
    print("🏷️ HEALING VIDEO CATEGORIES")
    if dry_run:
        print("   🔍 DRY RUN MODE - No changes will be made")
    if churches:
        print(f"   📂 Filtering to churches: {churches}")
    print("="*60)
    
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('_Summary.csv')]
    
    total_fixed = 0
    total_unchanged = 0
    category_changes = {}  # Track what changed to what
    
    for csv_file in sorted(csv_files):
        church_name = csv_file.replace('_Summary.csv', '').replace('_', ' ')
        
        # Filter by church if specified
        if churches:
            church_list = [c.strip().lower() for c in churches.split(',')]
            if not any(c in church_name.lower() for c in church_list):
                continue
        
        csv_path = os.path.join(data_dir, csv_file)
        
        try:
            # Read existing CSV
            rows = []
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            fixed_in_church = 0
            
            for row in rows:
                title = row.get('title', '')
                speaker = row.get('speaker', 'Unknown Speaker')
                description = row.get('description', '')
                old_type = row.get('type', 'Full Sermon')
                duration = int(row.get('duration', 0) or 0)
                
                # Create a mock yt_obj-like object for duration
                class MockYT:
                    def __init__(self, dur):
                        self.length = dur * 60  # Convert back to seconds
                
                mock_yt = MockYT(duration) if duration > 0 else None
                
                # Re-determine video type
                new_type = determine_video_type(title, speaker, None, mock_yt, description)
                
                if new_type != old_type:
                    change_key = f"{old_type} -> {new_type}"
                    category_changes[change_key] = category_changes.get(change_key, 0) + 1
                    
                    if not dry_run:
                        row['type'] = new_type
                    
                    fixed_in_church += 1
                else:
                    total_unchanged += 1
            
            if fixed_in_church > 0:
                print(f"   📂 {church_name}: {fixed_in_church} categories updated")
                
                # Write updated CSV
                if not dry_run:
                    fieldnames = ["date", "status", "speaker", "title", "url", "last_checked", "language", "type", "description", "duration", "church"]
                    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                        writer.writeheader()
                        writer.writerows(rows)
                
                total_fixed += fixed_in_church
            
        except Exception as e:
            print(f"   ❌ Error processing {csv_file}: {e}")
    
    print("\n" + "="*60)
    print("📊 CATEGORY HEALING SUMMARY")
    print(f"   ✅ Categories fixed: {total_fixed}")
    print(f"   ✓ Already correct: {total_unchanged}")
    
    if category_changes:
        print("\n   📋 Changes breakdown:")
        for change, count in sorted(category_changes.items(), key=lambda x: -x[1]):
            print(f"      {change}: {count}")
    
    print("="*60)
    
    return total_fixed, total_unchanged


# --- SPEAKER EXTRACTION PATTERN FUNCTIONS ---
def is_valid_name(name):
    """Alias for is_valid_person_name for compatibility."""
    return is_valid_person_name(name)

def remove_dates(text):
    """Remove date patterns from text."""
    for pattern in DATE_PATTERNS:
        text = re.sub(pattern, ' ', text, flags=re.IGNORECASE)
    return re.sub(r'\s+', ' ', text).strip()

def extract_speaker_pattern1(title):
    """Pattern 1: Honorific + Name - 'Bro. Ron Spencer', 'Pastor John Smith'"""
    pattern = rf'{HONORIFIC_PATTERN}[\s\.]+({NAME_PATTERN}(?:\s+{NAME_PATTERN})*)'
    matches = re.findall(pattern, title, re.IGNORECASE)
    for match in matches:
        name = clean_name(match)
        if is_valid_name(name):
            return name
    # Handle "Bro.Name.Name" pattern (no spaces)
    pattern2 = r'Bro\.([A-Z][a-z]+)\.([A-Z][a-z]+)'
    match = re.search(pattern2, title)
    if match:
        name = f"{match.group(1)} {match.group(2)}"
        name = clean_name(name)
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern2(title):
    """Pattern 2: '- Name on Date' or '- Name'"""
    pattern = rf'[-–—]\s*{HONORIFIC_PATTERN}[\s\.]+({NAME_PATTERN}(?:\s+{NAME_PATTERN})*)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    pattern = r'[-–—]\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s+(?:on|at)\s+'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern3(title):
    """Pattern 3: 'Name - Title' or 'Name: Title'"""
    pattern = r'^\d{6}\s*[-–—]?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*:\s*[A-Z]'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    pattern = r'[-–—]\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*:\s*[A-Z]'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern4(title):
    """Pattern 4: '|' separator patterns"""
    parts = re.split(r'\s*\|\s*', title)
    for part in parts:
        pattern = rf'{HONORIFIC_PATTERN}[\s\.]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)'
        match = re.search(pattern, part, re.IGNORECASE)
        if match:
            name = clean_name(match.group(1))
            if is_valid_name(name):
                return name
    if len(parts) >= 3:
        for part in parts[1:-1]:
            part = part.strip()
            if re.match(r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}$', part):
                name = clean_name(part)
                if is_valid_name(name):
                    return name
    return None

def extract_speaker_pattern5(title):
    """Pattern 5: Date prefix patterns"""
    pattern = rf'^\d{{2,4}}[-/]\d{{2}}[-/]\d{{2,4}}\s+{HONORIFIC_PATTERN}[\s\.]+({NAME_PATTERN}(?:\s+{NAME_PATTERN})*)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    pattern = rf'^\d{{6}}\s*[-–—]?\s*{HONORIFIC_PATTERN}[\s\.]+({NAME_PATTERN}(?:\s+{NAME_PATTERN})*)\s*:'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern6(title):
    """Pattern 6: 'by Honorific Name' patterns"""
    pattern = rf'(?:by|with|from)\s+{HONORIFIC_PATTERN}[\s\.]+({NAME_PATTERN}(?:\s+{NAME_PATTERN})*)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern7(title):
    """Pattern 7: Parenthetical speaker info"""
    pattern = rf'\({HONORIFIC_PATTERN}[\s\.]+({NAME_PATTERN}(?:\s+{NAME_PATTERN})*)\)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern8(title):
    """Pattern 8: Title is just a name"""
    clean_title = remove_dates(title)
    clean_title = re.sub(r'^\d{2,4}[-/]\d{2}[-/]\d{2,4}\s*', '', clean_title)
    clean_title = re.sub(r'\s*[-–—]\s*\d.*$', '', clean_title)
    clean_title = clean_title.strip()
    pattern = rf'^{HONORIFIC_PATTERN}[\s\.]+({NAME_PATTERN}(?:\s+{NAME_PATTERN})*)$'
    match = re.search(pattern, clean_title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    if re.match(r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3}$', clean_title):
        if is_valid_name(clean_title):
            return clean_title
    return None

def extract_speaker_pattern9(title):
    """Pattern 9: Name followed by topic"""
    pattern = r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*[-–—:]\s*[A-Z]'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name) and not re.match(r'^(?:The|A|An|This|Our|My|God|Jesus|Christ|Lord|Holy)\s', name, re.IGNORECASE):
            return name
    return None

def extract_speaker_pattern10(title):
    """Pattern 10: After quoted title"""
    pattern = rf'["\'"]+[^"\']+["\'"]+\s*[-–—]\s*{HONORIFIC_PATTERN}[\s\.]+({NAME_PATTERN}(?:\s+{NAME_PATTERN})*)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern11(title):
    """Pattern 11: Colon-separated patterns"""
    pattern = rf':\s*{HONORIFIC_PATTERN}[\s\.]+({NAME_PATTERN}(?:\s+{NAME_PATTERN})*)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    pattern = r'(?:Speaker|Minister|Preacher|Pastor|Guest)[\s:]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern12(title):
    """Pattern 12: Sunday School / Special formats"""
    pattern = rf'(?:Sunday\s+School|Bible\s+Study|Prayer\s+Meeting)[\s:]+{HONORIFIC_PATTERN}[\s\.]+({NAME_PATTERN}(?:\s+{NAME_PATTERN})*)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern13(title):
    """Pattern 13: Double-pipe patterns"""
    parts = re.split(r'\s*\|\|\s*', title)
    for part in parts:
        pattern = rf'{HONORIFIC_PATTERN}[\s\.]+({NAME_PATTERN}(?:\s+{NAME_PATTERN})*)'
        match = re.search(pattern, part, re.IGNORECASE)
        if match:
            name = clean_name(match.group(1))
            if is_valid_name(name):
                return name
    return None

def extract_speaker_pattern14(title):
    """Pattern 14: Bracket patterns"""
    clean_title = re.sub(r'\[[^\]]+\]', '', title)
    pattern = rf'{HONORIFIC_PATTERN}[\s\.]+({NAME_PATTERN}(?:\s+{NAME_PATTERN})*)'
    match = re.search(pattern, clean_title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern15(title):
    """Pattern 15: Complex date-name patterns"""
    pattern = rf'^\d{{2}}[-]?\d{{4}}\s*[-–—]?\s*({NAME_PATTERN}(?:\s+{NAME_PATTERN})*)\s*:'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    pattern = rf'\d{{4}}[-/]\d{{2}}[-/]\d{{2}}\s+{HONORIFIC_PATTERN}[\s\.]+({NAME_PATTERN}(?:\s+{NAME_PATTERN})*)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern16(title):
    """Pattern 16: Sermon Clip formats"""
    pattern = r'Sermon\s+Clips?[:\s]+\d{6}\s*[-–—]\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)[\s:]+[A-Z]'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    pattern = r'Sermon\s+Clips?\s*[-:]\s*\d{6}\s*[-–—]\s*([A-Z][a-z]+(?:\s+[A-Za-z][a-z]+)+)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern17(title):
    """Pattern 17: YYMMDD - Name - Title or YYMMDD-Name:Title"""
    pattern = r'^\d{6}\s*[-–—]\s*([A-Z][a-z]+(?:\s+(?:del\s+)?[A-Z][a-z]+)+)\s*[-–—]\s*[A-Z]'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    pattern = r'^\d{6}[-–—]([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*:'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern18(title):
    """Pattern 18: Honorific + Name followed by topic"""
    pattern = rf'^{HONORIFIC_PATTERN}[\s\.]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){{1,2}})\s+[A-Z][a-z]+'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            remaining = title[match.end(1):].strip()
            if len(remaining.split()) >= 2:
                return name
    return None

def extract_speaker_pattern19(title):
    """Pattern 19: Pastor Name followed by topic"""
    pattern = r'^(?:Pastor|Rev\.?|Reverend)[\s\.]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+))\s+[A-Z][a-z]+'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern20(title):
    """Pattern 20: Name after topic - Name"""
    pattern = r'[-–—]\s*([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*(?:\(|$|at\s)'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    pattern = r'[-–—]\s*([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*$'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern21(title):
    """Pattern 21: Parenthetical date format - Name after"""
    pattern = r'^\(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\)\s+.*[-–—]\s*([A-Z][a-z]+\s+[A-Z][a-z]+)'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern22(title):
    """Pattern 22: Topic: Name at end"""
    pattern = r':\s*([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*$'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern23(title):
    """Pattern 23: Wade Dale / Samuel Dale style"""
    pattern = r'^\d{2}[-]?\d{4}(?:am|pm)?\s*[-–—]\s*.*[-–—]\s*([A-Z][a-z]+\s+[A-Z][a-z]+)\s*$'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern24(title):
    """Pattern 24: Sermon Clip with different spacing"""
    pattern = r'Sermon\s+Clips?:\d{6}[ap]?\s*[-–—]\s*([A-Z][a-z]+\s+[A-Z][a-z]+)[\s:]+[A-Z]'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern25(title):
    """Pattern 25: -Past. / Pastor in Spanish"""
    pattern = r'[-–—]?\s*Past\.?\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern26(title):
    """Pattern 26: Minister pattern"""
    pattern = rf'/\s*(?:Minister|Visiting\s+Minister)?\s*{HONORIFIC_PATTERN}[\s\.]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern27(title):
    """Pattern 27: Sunday School date pattern"""
    pattern = r'\(Sunday\s+School\)\s*\d{6}\s*[-–—]\s*([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*:'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern28(title):
    """Pattern 28: Brother with apostrophe"""
    pattern = r"Brother\s+([A-Z][a-z]+(?:\s+[A-Z]'?[A-Za-z]+)+)"
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern29(title):
    """Pattern 29: Name with middle initial/suffix"""
    pattern = rf'{HONORIFIC_PATTERN}[\s\.]+([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:,?\s*(?:Sr\.?|Jr\.?|III|II|IV))?)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        name = re.sub(r'\s+[A-Z]\.\s*', ' ', name)
        name = re.sub(r',?\s*(?:Sr\.?|Jr\.?|III|II|IV)$', '', name)
        name = re.sub(r'\s+', ' ', name).strip()
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern30(title):
    """Pattern 30: Date MM/DD/YY(M) Name"""
    pattern = r'\d{1,2}/\d{1,2}/\d{2,4}[AaMm]?\s+([A-Z][a-z]+\s+[A-Z][a-z]+)'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern31(title):
    """Pattern 31: YY MMDD space separated then name"""
    pattern = r'^\d{2}\s+\d{4}(?:\s+\w+)+\s+([A-Z][a-z]+\s+[A-Z][a-z]+)\s*$'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern32(title):
    """Pattern 32: YYYY MMDDXX format"""
    pattern = r'^\d{4}\s+\d{4}[AaPpMm]{0,2}\s+.*[-–—,]\s*(?:Pst\.?|Pastor)?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        name = re.sub(r',\s*[A-Z][a-z]+$', '', name)
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern33(title):
    """Pattern 33: Topic Part X - Name"""
    pattern = r'(?:Part|Pt\.?)\s*\d+\s*[-–—]\s*([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*$'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern34(title):
    """Pattern 34: Sisters' patterns"""
    pattern = r"Sisters?['\s]+([A-Z][a-z]+(?:\s+and\s+[A-Z][a-z]+)?)"
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern35(title):
    """Pattern 35: fr/Bro French style"""
    pattern = r'[-–—,\s]+(?:fr|Br|Bro)\.?\s+([A-Z][a-zéèêëàâäôöùûüç]+(?:\s+[A-Z][a-z]+)*)'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern36(title):
    """Pattern 36: YYMMDD[ap] - Name: Title"""
    pattern = r'^\d{6}[ap]?\s*[-–—]\s*([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*:'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name) and not re.match(r'^(?:Rapturing|Celebrating|Receiving|Eve)\s', name):
            return name
    return None

def extract_speaker_pattern37(title):
    """Pattern 37: YYMMDD: Name - Title"""
    pattern = r'^\d{6}\s*:\s*([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*[-–—]'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern38(title):
    """Pattern 38: YYMMDD - Name: Topic"""
    pattern = r'^\d{6}\s+[-–—]\s+([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*:'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern39(title):
    """Pattern 39: 'by Name' at end"""
    pattern = r'\bby\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*$'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern40(title):
    """Pattern 40: Sermon Clip various formats"""
    pattern = r'Sermon\s+Clip\s*\d*\s*:\s*\d{5,6}\s*[-–—]\s*([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*:'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    pattern = r'Sermon\s+Clip\s*:\s*\d{6}[ap]?\s+([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*:'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    pattern = r'Sermon\s+Clip\s*:\s*([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*:'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern41(title):
    """Pattern 41: YYYY-MMDD ... - Hno. Name (Spanish)"""
    pattern = rf'\d{{4}}[-]?\d{{4}}\s+.*[-–—]\s*{HONORIFIC_PATTERN}[\s\.]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern42(title):
    """Pattern 42: WMB / William Branham patterns"""
    if re.search(r'[-–—]\s*(?:Rev\.?\s+)?WMB\s*$', title, re.IGNORECASE):
        return "William M. Branham"
    if re.search(r'[-–—]\s*Rev\.\s+William\s+M\.?\s+Branham', title, re.IGNORECASE):
        return "William M. Branham"
    pattern = r'Taped\s+Sermon\s*:\s*William\s+(?:M\.?\s+)?(?:Marrion\s+)?Branham\s*:'
    if re.search(pattern, title, re.IGNORECASE):
        return "William M. Branham"
    if re.search(r'Prophet\s+William\s+(?:M\.?\s+)?(?:Marrion\s+)?Branham', title, re.IGNORECASE):
        return "William M. Branham"
    if re.search(r'[-–—]\s*(?:Rev\.?\s+)?William\s+(?:M\.?\s+)?(?:Marrion\s+)?Branham\s*$', title, re.IGNORECASE):
        return "William M. Branham"
    return None

def extract_speaker_pattern43(title):
    """Pattern 43: Date | ... Pst. Name"""
    pattern = rf'\d{{2}}[-/]\d{{2}}[-/]\d{{2,4}}\s*\|.*[-–—:]\s*{HONORIFIC_PATTERN}[\s\.]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern44(title):
    """Pattern 44: Name with lowercase middle (Danny del Mundo)"""
    pattern = r'^\d{6}\s*[-–—]\s*([A-Z][a-z]+\s+(?:del\s+|de\s+|van\s+|von\s+)?[A-Z][a-z]+)\s*:'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern45(title):
    """Pattern 45: Topic - Name - 1 of 2"""
    pattern = r'[-–—]\s*([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*[-–—\[\(]\s*(?:Part\s*)?\d+\s+of\s+\d+'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern46(title):
    """Pattern 46: Name Suffix (Minister Meeting, etc.)"""
    pattern = r'[-–—]\s*([A-Z][a-z]+\s+[A-Z][a-z]+)\s+(?:Minister\s+Meeting|Youth\s+Service|Fellowship|Convention)$'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern47(title):
    """Pattern 47: Hno's. Name & Name (multiple speakers)"""
    pattern = r"Hno'?s?\.?\s+([A-Z][a-z]+\s+[A-Z][a-z]+)\s*[&]\s*([A-Z][a-z]+\s+[A-Z][a-z]+)"
    match = re.search(pattern, title)
    if match:
        name1 = clean_name(match.group(1))
        name2 = clean_name(match.group(2))
        if is_valid_name(name1) and is_valid_name(name2):
            return f"{name1} & {name2}"
    return None

def extract_speaker_pattern47b(title):
    """Pattern 47b: Multi-speaker with different honorifics
    Examples: 'Bro. David and Sis. Faith Kegley', 'Bro Tobi and Bro Feran'
    """
    # Pattern: Honorific Name and Honorific Name
    pattern = r'(?:Bro\.?|Brother|Sis\.?|Sister|Pastor)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+and\s+(?:Bro\.?|Brother|Sis\.?|Sister|Pastor)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name1 = clean_name(match.group(1))
        name2 = clean_name(match.group(2))
        if name1 and name2:
            return f"{name1} & {name2}"
    # Also handle "&" instead of "and"
    pattern2 = r'(?:Bro\.?|Brother|Sis\.?|Sister|Pastor)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*&\s*(?:Bro\.?|Brother|Sis\.?|Sister|Pastor)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'
    match2 = re.search(pattern2, title, re.IGNORECASE)
    if match2:
        name1 = clean_name(match2.group(1))
        name2 = clean_name(match2.group(2))
        if name1 and name2:
            return f"{name1} & {name2}"
    return None

def extract_speaker_pattern48(title):
    """Pattern 48: Bro. Name (single name) followed by dash, end, or & Choir"""
    pattern = r'(?:Bro\.?|Brother|Sis\.?|Sister|Pastor)\s+([A-Z][a-z]+)(?:\s*[-–—]|\s*&\s*(?:Choir|Saints)|\s*$)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = match.group(1).strip()
        if name and len(name) > 1 and name[0].isupper():
            if name.lower() not in ['the', 'and', 'for', 'with', 'from', 'god']:
                return name
    return None

def extract_speaker_pattern49(title):
    """Pattern 49: -Sis. Name (no space before honorific)"""
    pattern = r'-(?:Sis|Bro|Sister|Brother)\.?\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        name = re.sub(r'\s*&\s*Saints\.?$', '', name, flags=re.IGNORECASE)
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern50(title):
    """Pattern 50: Founding Pastor"""
    pattern = r'Founding\s+Pastor\s*[-|]\s*([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:\s+(?:Sr|Jr|III|IV)\.?)?)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern51(title):
    """Pattern 51: X Sisters group names"""
    pattern = r'(?:The\s+)?([A-Z][a-z]+)\s+Sisters'
    match = re.search(pattern, title)
    if match:
        name = match.group(1)
        if name.lower() not in ['the', 'and', 'our', 'all', 'some']:
            return f"{name} Sisters"
    return None

def extract_speaker_pattern52(title):
    """Pattern 52: Pastor Name. LastName (typo period)"""
    pattern = r'(?:Pastor|Bro\.?|Brother)\s+([A-Z][a-z]+)\.\s+([A-Z][a-z]+)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = f"{match.group(1)} {match.group(2)}"
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern53(title):
    """Pattern 53: Pastor Initial LastName"""
    pattern = r'(?:Pastor|Bro\.?|Brother)\s+([A-Z](?:\.[A-Z])?\.?)\s+([A-Z][a-z]+)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        initial = match.group(1).rstrip('.')
        lastname = match.group(2)
        return f"{initial} {lastname}"
    return None

def extract_speaker_pattern54(title):
    """Pattern 54: Brothers' Name and Name"""
    pattern = r"(?:Brothers?'?|Sisters?'?)\s+([A-Z][a-z]+(?:(?:\s*,\s*|\s+and\s+|\s*&\s*)[A-Z][a-z]+)*)"
    match = re.search(pattern, title)
    if match:
        names = match.group(1)
        names = re.sub(r'\s+and\s+', ', ', names)
        names = re.sub(r'\s*&\s*', ', ', names)
        return names
    return None

def extract_speaker_pattern55(title):
    """Pattern 55: Sis. Name S. (with initial at end)"""
    pattern = r'(?:Sis\.|Sister|Bro\.|Brother)\s+([A-Z][a-z]+)\s+([A-Z])\.?\s*$'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        return f"{match.group(1)} {match.group(2)}."
    return None

def extract_speaker_pattern56(title):
    """Pattern 56: YYYY-MM-DD Bro. Name - Topic"""
    pattern = r'\d{4}-\d{2}-\d{2}\s+(?:Bro\.|Brother|Sis\.|Sister|Pastor)\s+([A-Z][a-z]+)\s*[-–—]'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = match.group(1)
        if name and len(name) > 2:
            return name
    return None

def extract_speaker_pattern57(title):
    """Pattern 57: Sis. joy & Happiness Name"""
    pattern = r'(?:Sis\.|Sister|Bro\.|Brother)\s+([A-Za-z]+(?:\s*&\s*[A-Z][a-z]+)*\s+[A-Z][a-z]+)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name = match.group(1)
        if name and name[0].islower():
            name = name[0].upper() + name[1:]
        return name
    return None

def extract_speaker_pattern58(title):
    """Pattern 58: Bro. Name & Choir"""
    pattern = r'(?:Bro\.|Brother|Sis\.|Sister)\s+([A-Z][a-z]+)\s*&\s*(?:Choir|Saints)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

def extract_speaker_pattern59(title):
    """Pattern 59: Sister Name Name"""
    pattern = r'(?<![A-Za-z])Sister\s+([A-Z][a-z]+\s+[A-Z][a-z]+)'
    match = re.search(pattern, title)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    return None

def extract_speaker_pattern60(title):
    """Pattern 60: Bro. Name O. with initial at end before dash"""
    pattern = r'(?:Bro\.|Brother|Sis\.|Sister)\s+([A-Z][a-z]+)\s+([A-Z])\.?\s*[-–—]'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        return f"{match.group(1)} {match.group(2)}."
    return None

def extract_speaker_pattern61(title):
    """Pattern 61: Bro. Ovid, Headstone"""
    pattern = r'(?:Bro\.|Brother|Pastor)\s+([A-Z][a-z]+),\s*(?:Headstone|[A-Z][a-z]+\s+(?:Tabernacle|Church))'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

def extract_speaker_pattern62(title):
    """Pattern 62: Pastor Name (Country)"""
    pattern = r'Pastor\s+([A-Z][a-z]+)\s*\([A-Z][a-z]+\)'
    match = re.search(pattern, title)
    if match:
        return match.group(1)
    return None

def extract_speaker_pattern63(title):
    """Pattern 63: Bro. Name + Something"""
    pattern = r'(?:Bro\.|Brother)\s+([A-Z][a-z]+)\s*\+'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

def extract_speaker_pattern64(title):
    """Pattern 64: Topic: Bro. Name Date"""
    pattern = r':\s*(?:Bro\.|Brother|Sis\.|Sister)\s+([A-Z][a-z]+)\s+\d'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

def extract_speaker_pattern65(title):
    """Pattern 65: -Bro. Name Date"""
    pattern = r'-(?:Bro\.|Brother|Sis\.|Sister)\s+([A-Z][a-z]+)\s+\d'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

def extract_speaker_pattern66(title):
    """Pattern 66: Sr Name & Sisters"""
    pattern = r'Sr\.?\s+([A-Z][a-z]+)\s*&\s*Sisters'
    match = re.search(pattern, title)
    if match:
        return match.group(1)
    return None

def extract_speaker_pattern67(title):
    """Pattern 67: Sr. Name -"""
    pattern = r'Sr\.\s+([A-Z][a-z]+)\s*[-–—]'
    match = re.search(pattern, title)
    if match:
        return match.group(1)
    return None

def extract_speaker_pattern68(title):
    """Pattern 68: Bro. Name & S Name Name"""
    pattern = r'(?:Bro\.|Brother)\s+([A-Z][a-z]+)\s*&\s*(?:S\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        return f"{match.group(1)} & {match.group(2)}"
    return None

def extract_speaker_pattern69(title):
    """Pattern 69: Pastor Name |"""
    pattern = r'Pastor\s+([A-Z][a-z]+)\s*\|'
    match = re.search(pattern, title)
    if match:
        return match.group(1)
    return None

def extract_speaker_pattern70(title):
    """Pattern 70: Generic multi-speaker patterns with &"""
    pattern = r'(?:Bro\.|Brother|Sis\.|Sister|Pastor)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+and\s+(?:Bro\.|Brother|Sis\.|Sister|Pastor)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name1 = clean_name(match.group(1))
        name2 = clean_name(match.group(2))
        if name1 and name2:
            return f"{name1} & {name2}"
    pattern2 = r'(?:Bro\.|Brother|Sis\.|Sister|Pastor)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*&\s*(?:Bro\.|Brother|Sis\.|Sister|Pastor)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'
    match2 = re.search(pattern2, title, re.IGNORECASE)
    if match2:
        name1 = clean_name(match2.group(1))
        name2 = clean_name(match2.group(2))
        if name1 and name2:
            return f"{name1} & {name2}"
    return None

def normalize_text(text):
    """Normalize unicode and clean up text."""
    if not text or not isinstance(text, str):
        return ""
    import unicodedata
    # Normalize unicode
    text = unicodedata.normalize('NFKD', text)
    # Replace smart quotes and other special chars
    text = text.replace('"', '"').replace('"', '"').replace(''', "'").replace(''', "'")
    text = text.replace('–', '-').replace('—', '-')
    return text.strip()

def final_validation(speaker, title=""):
    """Final validation and normalization of speaker name - enhanced version."""
    if not speaker or speaker == "Unknown Speaker":
        return speaker
    
    speaker = speaker.strip()
    
    # Handle "By Name" pattern FIRST - extract just the name
    by_match = re.match(r'^By\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)$', speaker)
    if by_match:
        extracted = by_match.group(1)
        if re.match(r'^[A-Z][a-z]+\s+[A-Z][a-z]+', extracted):
            speaker = extracted
        else:
            return "Unknown Speaker"
    
    # Block names starting with prepositions/articles
    if re.match(r'^(?:in|to|of|by|on|at|for|and|the)\s+', speaker, re.IGNORECASE):
        return "Unknown Speaker"
    
    # Block names ending with "and" or "&" (incomplete multi-speaker)
    if re.search(r'\s+(?:and|&)\s*$', speaker, re.IGNORECASE):
        return "Unknown Speaker"
    
    # Block "School" as a standalone name
    if speaker.lower() == 'school':
        return "Unknown Speaker"
    
    # Block names containing time/service words
    time_words = ['Evening', 'Morning', 'Night', 'Afternoon', 'Midnight']
    for word in time_words:
        if word.lower() in speaker.lower():
            if not re.match(r'^[A-Z][a-z]+\s+' + word + r'$', speaker, re.IGNORECASE):
                return "Unknown Speaker"
    
    # Block names ending with service-related words
    if re.search(r'\b(?:Service|Meeting|Worship|Testimonies|Consequences|Chronicles|Promises|Word)$', speaker, re.IGNORECASE):
        return "Unknown Speaker"
    
    # Block obvious non-name patterns that slipped through
    bad_patterns = [
        r'^Watchnight$', r'^Controlling\s+', r'^Everlasting\s+', r'^Witnessing\s+',
        r'^Atmosphere\s+', r'^Misunderstanding\s+', r'^Grace\s+and$',
        r'Yahweh\s+.*\s+Worship', r'^For\s+Us$', r'^By\s+Me$', r'^Of\s+Time$',
        r'^Of\s+Those\b', r'^Of\s+His\b', r'^Of\s+Revelation\b', r'^To\s+Know\b',
        r'^To\s+His\b', r'^And\s+The\b', r'and\s+the\s+Prostitute',
        r'^at\s+Tucson', r'^to\s+Chronicles',
    ]
    for pattern in bad_patterns:
        if re.search(pattern, speaker, re.IGNORECASE):
            return "Unknown Speaker"
    
    # Apply normalization
    speaker = normalize_speaker(speaker, title)
    if not speaker:
        return "Unknown Speaker"
    
    # Clean the name
    speaker = clean_name(speaker)
    if not speaker:
        return "Unknown Speaker"
    
    # Validate
    if not is_valid_name(speaker):
        # Try one more time with just the first two words
        words = speaker.split()
        if len(words) >= 2:
            short_name = " ".join(words[:2])
            if is_valid_name(short_name):
                return short_name
        return "Unknown Speaker"
    
    return speaker

# List of all pattern extraction functions in order
SPEAKER_PATTERN_FUNCTIONS = [
    extract_speaker_pattern42,  # William Branham patterns first (priority)
    extract_speaker_pattern1,   # Honorific + Name
    extract_speaker_pattern2,   # - Name on Date
    extract_speaker_pattern3,   # Name - Title
    extract_speaker_pattern4,   # Pipe separator
    extract_speaker_pattern5,   # Date prefix
    extract_speaker_pattern6,   # by Honorific Name
    extract_speaker_pattern7,   # Parenthetical
    extract_speaker_pattern10,  # After quoted title
    extract_speaker_pattern11,  # Colon-separated
    extract_speaker_pattern12,  # Sunday School
    extract_speaker_pattern13,  # Double-pipe
    extract_speaker_pattern14,  # Bracket patterns
    extract_speaker_pattern15,  # Complex date-name
    extract_speaker_pattern16,  # Sermon Clip
    extract_speaker_pattern17,  # YYMMDD - Name - Title
    extract_speaker_pattern18,  # Honorific + Name + topic
    extract_speaker_pattern19,  # Pastor Name + topic
    extract_speaker_pattern20,  # Name after topic
    extract_speaker_pattern21,  # Parenthetical date
    extract_speaker_pattern22,  # Topic: Name
    extract_speaker_pattern23,  # Wade Dale style
    extract_speaker_pattern24,  # Sermon Clip spacing
    extract_speaker_pattern25,  # Past. Spanish
    extract_speaker_pattern26,  # Minister pattern
    extract_speaker_pattern27,  # Sunday School date
    extract_speaker_pattern28,  # Brother apostrophe
    extract_speaker_pattern29,  # Middle initial
    extract_speaker_pattern30,  # MM/DD/YY Name
    extract_speaker_pattern31,  # YY MMDD Name
    extract_speaker_pattern32,  # YYYY MMDDXX
    extract_speaker_pattern33,  # Part X - Name
    extract_speaker_pattern34,  # Sisters'
    extract_speaker_pattern35,  # French style
    extract_speaker_pattern36,  # YYMMDD[ap]
    extract_speaker_pattern37,  # YYMMDD:
    extract_speaker_pattern38,  # YYMMDD - Name:
    extract_speaker_pattern39,  # by Name end
    extract_speaker_pattern40,  # Sermon Clip various
    extract_speaker_pattern41,  # Spanish YYYY-MMDD
    extract_speaker_pattern43,  # Date | Pst.
    extract_speaker_pattern44,  # del/de names
    extract_speaker_pattern45,  # Name - 1 of 2
    extract_speaker_pattern46,  # Name Suffix
    extract_speaker_pattern47,  # Hno's multiple
    extract_speaker_pattern47b, # Multi-speaker: Bro Name and Sis Name
    extract_speaker_pattern70,  # Multi-speaker &
    extract_speaker_pattern48,  # Single name Bro.
    extract_speaker_pattern49,  # -Sis.
    extract_speaker_pattern50,  # Founding Pastor
    extract_speaker_pattern51,  # X Sisters
    extract_speaker_pattern52,  # Name. Typo
    extract_speaker_pattern53,  # Initial LastName
    extract_speaker_pattern54,  # Brothers'
    extract_speaker_pattern55,  # Name S.
    extract_speaker_pattern56,  # YYYY-MM-DD Bro.
    extract_speaker_pattern57,  # joy & Happiness
    extract_speaker_pattern58,  # Name & Choir
    extract_speaker_pattern59,  # Sister Name Name
    extract_speaker_pattern60,  # Name O. -
    extract_speaker_pattern61,  # Ovid, Headstone
    extract_speaker_pattern62,  # Name (Country)
    extract_speaker_pattern63,  # Name +
    extract_speaker_pattern64,  # : Bro. Name Date
    extract_speaker_pattern65,  # -Bro. Name Date
    extract_speaker_pattern66,  # Sr & Sisters
    extract_speaker_pattern67,  # Sr. Name -
    extract_speaker_pattern68,  # Name & S Name
    extract_speaker_pattern69,  # Pastor Name |
    extract_speaker_pattern8,   # Just a name (last resort)
    extract_speaker_pattern9,   # Name: Topic (last resort)
]

# --- MAIN LOGIC ---
def is_boilerplate_description(description):
    """Check if description is a boilerplate message that won't contain speaker info."""
    if not description:
        return True
    desc = description.strip()
    boilerplate_starts = [
        'If these messages have blessed you',
        'Online',
        'Sunday Online',
        'Thank you for watching',
        'Please subscribe',
        'Like and subscribe',
        'Follow us on',
        'Visit our website',
        'For more information',
        'Connect with us',
    ]
    for start in boilerplate_starts:
        if desc.startswith(start):
            return True
    boilerplate_contains = [
        'We do not own the rights',
        'All rights reserved',
        'Copyright',
        '©',
    ]
    for phrase in boilerplate_contains:
        if phrase in desc:
            return True
    return False

def extract_speaker_from_description(description):
    """
    Extract speaker name from YouTube video description.
    Descriptions often have speaker info in specific formats.
    """
    if not description or is_boilerplate_description(description):
        return None
    
    # Skip if description mentions "Interpreter" - that's not the main speaker
    if 'Interpreter' in description or 'interpreter' in description:
        return None
    
    # Check for explicit speaker indicators in description
    # Pattern: "Speaker: Name" or "Preacher: Name" or "Minister: Name"
    speaker_label_pattern = r'(?:Speaker|Preacher|Minister|Pastor|Ministered by)[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)'
    match = re.search(speaker_label_pattern, description, re.IGNORECASE)
    if match:
        name = clean_name(match.group(1))
        if is_valid_name(name):
            return name
    
    # Check for honorific + name at start of description
    if description.strip().startswith(('Pst.', 'Past.', 'Pastor', 'Bro.', 'Brother', 'Sis.', 'Sister')):
        pattern = rf'^{HONORIFIC_PATTERN}[\s\.]+({NAME_PATTERN}(?:\s+{NAME_PATTERN})*)'
        match = re.search(pattern, description.strip(), re.IGNORECASE)
        if match:
            name = clean_name(match.group(1))
            if is_valid_name(name):
                return name
    
    # Try the first few lines of description (often has speaker info)
    first_lines = description.split('\n')[:3]
    for line in first_lines:
        line = line.strip()
        if not line:
            continue
        # Try pattern extraction on each line
        for pattern_func in SPEAKER_PATTERN_FUNCTIONS[:30]:  # Use top 30 patterns
            try:
                result = pattern_func(line)
                if result:
                    validated = final_validation(result, line)
                    if validated and validated != "Unknown Speaker":
                        return validated
            except Exception:
                continue
    
    return None

def identify_speaker_dynamic(title, description, known_speakers):
    """
    Enhanced speaker identification using 70+ pattern extraction functions.
    """
    # First check Tucson Tabernacle hardcoded speakers (scraped from their website)
    if title in TUCSON_TABERNACLE_SPEAKERS:
        return TUCSON_TABERNACLE_SPEAKERS[title], True
    
    # Also check with normalized whitespace (TT titles sometimes have extra spaces)
    normalized_title = re.sub(r'\s+', ' ', title).strip()
    for tt_title, tt_speaker in TUCSON_TABERNACLE_SPEAKERS.items():
        if re.sub(r'\s+', ' ', tt_title).strip() == normalized_title:
            return tt_speaker, True
    
    # Check for known speakers in text (title + description)
    found_speakers = set()
    search_text = f"{title}\n{description}"
    for name in known_speakers:
        if name in search_text:
            found_speakers.add(name)
    
    # Try all pattern extraction functions on title in order
    title_speaker = None
    for pattern_func in SPEAKER_PATTERN_FUNCTIONS:
        try:
            result = pattern_func(title)
            if result:
                # Apply final validation
                validated = final_validation(result, title)
                if validated and validated != "Unknown Speaker":
                    title_speaker = normalize_speaker(validated, title)
                    break
        except Exception:
            continue
    
    # If we found a speaker from title, verify/enhance with description
    if title_speaker:
        desc_speaker = extract_speaker_from_description(description)
        if desc_speaker:
            # If title detection looks like a topic (not a real name), prefer description
            if re.match(r'^[A-Z][a-z]+\s+(?:Of|To|In|The|And|With|For|By|At|On)\s+', title_speaker):
                return normalize_speaker(desc_speaker, title), True
            # If title has just a first name and description has full name, prefer description
            if len(title_speaker.split()) == 1 and len(desc_speaker.split()) >= 2:
                if title_speaker.lower() in desc_speaker.lower():
                    return normalize_speaker(desc_speaker, title), True
        return title_speaker, True
    
    # No speaker from title - try description with full pattern matching
    desc_speaker = extract_speaker_from_description(description)
    if desc_speaker:
        return normalize_speaker(desc_speaker, title), False
    
    # Try more patterns on description as fallback
    if description and not is_boilerplate_description(description):
        for pattern_func in SPEAKER_PATTERN_FUNCTIONS[:40]:  # Use first 40 patterns on description
            try:
                result = pattern_func(description)
                if result:
                    validated = final_validation(result, title)
                    if validated and validated != "Unknown Speaker":
                        return normalize_speaker(validated, title), False
            except Exception:
                continue
    
    # Fallback: Check for any known speaker match
    if found_speakers:
        final_list = consolidate_names(found_speakers)
        normalized_list = [normalize_speaker(s, title) for s in final_list]
        normalized_list = [s for s in normalized_list if s]  # Remove empty
        normalized_list = sorted(list(set(normalized_list)))
        if normalized_list:
            return ", ".join(normalized_list), False
    
    # Last resort: try separator-based extraction on title
    separators = [r'\s[-–—]\s', r'\s[:|]\s', r'\sby\s']
    for sep in separators:
        parts = re.split(sep, title)
        if len(parts) > 1:
            candidate = clean_name(parts[0].strip())
            if is_valid_person_name(candidate):
                validated = final_validation(candidate, title)
                if validated and validated != "Unknown Speaker":
                    return normalize_speaker(validated, title), False
            candidate_end = clean_name(parts[-1].strip())
            if is_valid_person_name(candidate_end):
                validated = final_validation(candidate_end, title)
                if validated and validated != "Unknown Speaker":
                    return normalize_speaker(validated, title), False
    
    return "Unknown Speaker", False

def determine_video_type(title, speaker, transcript_text=None, yt_obj=None, description=""):
    title_lower = title.lower()
    desc_lower = (description or "").lower()
    text_to_search = title_lower + " " + desc_lower  # Combined search text
    
    # Get duration if available
    duration_minutes = 0
    if yt_obj:
        try:
            duration_seconds = yt_obj.length if hasattr(yt_obj, 'length') else 0
            duration_minutes = duration_seconds / 60 if duration_seconds else 0
        except:
            pass
    
    # Tape Service - William Branham recordings
    if speaker == "William M. Branham": return "Tape Service"
    
    # Memorial/Funeral Services
    if any(term in text_to_search for term in ["memorial", "celebrating the life", "funeral", "home going", "homegoing", "tribute", "in memory", "in loving memory"]): 
        return "Memorial Service"
    
    # Wedding
    if any(term in text_to_search for term in ["wedding", "marriage ceremony"]): 
        return "Wedding"
    
    # Baby/Child Dedication
    if any(term in text_to_search for term in ["baby dedication", "child dedication", "dedication service"]):
        return "Dedication Service"
    
    # Baptism Services
    if any(term in text_to_search for term in ["baptism", "baptismal", "water service"]):
        return "Baptism Service"
    
    # Communion/Lord's Supper
    if any(term in text_to_search for term in ["communion", "lord's supper", "lords supper", "foot washing", "footwashing"]):
        return "Communion Service"
    
    # Youth/Children's Programs
    if any(term in text_to_search for term in ["youth camp", "youth service", "youth meeting"]):
        return "Youth Service"
    if any(term in text_to_search for term in ["sunday school", "children's", "childrens"]) or ("kids" in title_lower):
        return "Sunday School"
    
    # Special Holiday Programs
    if any(term in text_to_search for term in ["christmas", "nativity"]): 
        return "Christmas Program"
    if any(term in text_to_search for term in ["easter", "resurrection sunday"]) or ("resurrection" in title_lower):
        return "Easter Program"
    if "thanksgiving" in text_to_search:
        return "Thanksgiving Service"
    if "new year" in text_to_search:
        return "New Year Service"
    
    # Convention/Camp Meeting
    if any(term in text_to_search for term in ["convention", "camp meeting", "campmeeting"]):
        return "Convention"
    
    # Prayer Meeting/Bible Study
    if any(term in text_to_search for term in ["prayer meeting", "prayer service"]):
        return "Prayer Meeting"
    if any(term in text_to_search for term in ["bible study", "bible class"]):
        return "Bible Study"
    
    # Q&A Sessions
    if any(term in text_to_search for term in ["q&a", "q & a", "questions and answers", "question and answer"]):
        return "Q&A Session"
    
    # Testimony Service
    if any(term in text_to_search for term in ["testimony", "testimonies"]):
        return "Testimony Service"
    
    # Sermon Clip
    if any(term in text_to_search for term in ["clip", "excerpt", "highlight"]): 
        return "Sermon Clip"
    
    # Song Special - explicit title/description match first
    if any(term in text_to_search for term in ["song special", "special song"]):
        return "Song Special"
    
    # Worship Service - explicit title/description match
    if "worship service" in text_to_search:
        return "Worship Service"
    
    # Song Special - short videos with high [Music] ratio
    # Check title hints first
    song_title_hints = ["special" in title_lower and ("song" in title_lower or "music" in title_lower or "sing" in title_lower),
                        "solo" in title_lower,
                        "duet" in title_lower,
                        "quartet" in title_lower,
                        "choir" in title_lower and "special" in title_lower,
                        "hymn" in title_lower and len(title) < 60]
    if any(song_title_hints):
        return "Song Special"
    
    # Analyze transcript and duration for Song Special detection
    if transcript_text and duration_minutes > 0:
        try:
            # Count [Music] tags and total words
            music_count = transcript_text.lower().count('[music]') + transcript_text.lower().count('(music)')
            word_count = len(transcript_text.split())
            
            # Song Special criteria:
            # - Under 20 minutes
            # - High music ratio (more than 1 music tag per 100 words, or very short with any music)
            if duration_minutes < 20:
                if word_count > 0:
                    music_ratio = music_count / (word_count / 100)  # Music tags per 100 words
                    # Short video (< 10 min) with significant music presence
                    if duration_minutes < 10 and music_count >= 2 and music_ratio > 0.5:
                        return "Song Special"
                    # Medium short video (10-20 min) with high music ratio
                    if duration_minutes < 20 and music_ratio > 1.0:
                        return "Song Special"
                # Very short transcript with music tags likely a song
                if word_count < 500 and music_count >= 2:
                    return "Song Special"
        except:
            pass
    
    # Worship/Song Service (only if no identified speaker) - full-length worship sessions
    if any(term in text_to_search for term in ["worship", "song service", "praise service", "singing"]) and "song of solomon" not in text_to_search:
        if speaker == "Unknown Speaker": 
            return "Worship Service"
    
    # Full Sermon - videos 60+ minutes with identified speaker
    if duration_minutes >= 60:
        return "Full Sermon"
    
    # Short Service - videos under 60 minutes (not matching other categories)
    if duration_minutes > 0 and duration_minutes < 60:
        return "Short Service"
    
    # Default when duration unknown
    return "Service"

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

def extract_spoken_word_date(text):
    """
    Extract date from Spoken Word Church title format: YY-MMDD(am/pm)
    Examples: 10-0704, 08-1214, 09-0521pm, 10-0411am
    
    Returns: date string in YYYY-MM-DD format, or None if not matched
    """
    match = re.match(r'^(\d{2})-(\d{2})(\d{2})(am|pm)?', text.strip(), re.IGNORECASE)
    if not match:
        return None
    
    year_2digit = int(match.group(1))
    month = match.group(2)
    day = match.group(3)
    
    # Convert 2-digit year (assumes 2000s for 00-29, 1900s for 30-99)
    year_4digit = 2000 + year_2digit if year_2digit <= 29 else 1900 + year_2digit
    
    try:
        month_int, day_int = int(month), int(day)
        if 1 <= month_int <= 12 and 1 <= day_int <= 31:
            dt = datetime.datetime(year_4digit, month_int, day_int)
            return dt.strftime("%Y-%m-%d")
    except (ValueError, OverflowError):
        pass
    return None

def extract_date_from_text(text):
    # First try Spoken Word Church YY-MMDD format (must be at start of title)
    spoken_word_date = extract_spoken_word_date(text)
    if spoken_word_date: return spoken_word_date
    
    # Standard YYYY-MM-DD or YYYY/MM/DD format
    match = re.search(r'(\d{4})[-./](\d{2})[-./](\d{2})', text)
    if match: return validate_year(f"{match.group(1)}-{match.group(2)}-{match.group(3)}")
    # MM-DD-YYYY format
    match = re.search(r'(\d{2})[-./](\d{2})[-./](\d{4})', text)
    if match: return validate_year(f"{match.group(3)}-{match.group(1)}-{match.group(2)}")
    # YYMMDD format (compact)
    match = re.search(r'\b(2[0-9])(\d{2})(\d{2})\b', text)
    if match:
        year = 2000 + int(match.group(1))
        return validate_year(f"{year}-{match.group(2)}-{match.group(3)}")
    # Month Day, Year format (e.g., "January 15, 2020")
    match = re.search(r'([A-Z][a-z]+)\s+(\d{1,2}),?\s+(\d{4})', text)
    if match:
        try:
            dt = datetime.datetime.strptime(f"{match.group(1)} {match.group(2)} {match.group(3)}", "%B %d %Y")
            return validate_year(dt.strftime("%Y-%m-%d"))
        except: pass
    return None

def extract_streamed_live_date(text):
    """
    Extract date from "Streamed live on" or similar phrases in YouTube descriptions.
    YouTube uses formats like:
    - "Streamed live on Mar 15, 2023"
    - "Streamed live on March 15, 2023"
    - "streamed on Sunday 25th August, 2019"
    - "was streamed on 14 May 2023"
    
    Returns: date string in YYYY-MM-DD format, or None if not matched
    """
    if not text:
        return None
    
    # Pattern 1: "Streamed live on Month Day, Year" or "streamed on Month Day, Year"
    match = re.search(r'[Ss]treamed\s+(?:live\s+)?on\s+([A-Z][a-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})', text)
    if match:
        try:
            month_str, day, year = match.group(1), match.group(2), match.group(3)
            dt = datetime.datetime.strptime(f"{month_str} {day} {year}", "%B %d %Y")
            return validate_year(dt.strftime("%Y-%m-%d"))
        except ValueError:
            # Try abbreviated month
            try:
                dt = datetime.datetime.strptime(f"{month_str} {day} {year}", "%b %d %Y")
                return validate_year(dt.strftime("%Y-%m-%d"))
            except ValueError:
                pass
    
    # Pattern 2: "streamed on Day Month Year" (e.g., "streamed on 14 May 2023")
    match = re.search(r'[Ss]treamed\s+(?:live\s+)?on\s+(\d{1,2})(?:st|nd|rd|th)?\s+([A-Z][a-z]+),?\s+(\d{4})', text)
    if match:
        try:
            day, month_str, year = match.group(1), match.group(2), match.group(3)
            dt = datetime.datetime.strptime(f"{day} {month_str} {year}", "%d %B %Y")
            return validate_year(dt.strftime("%Y-%m-%d"))
        except ValueError:
            try:
                dt = datetime.datetime.strptime(f"{day} {month_str} {year}", "%d %b %Y")
                return validate_year(dt.strftime("%Y-%m-%d"))
            except ValueError:
                pass
    
    # Pattern 3: "streamed on Sunday 25th August, 2019" (with day of week)
    match = re.search(r'[Ss]treamed\s+(?:live\s+)?on\s+(?:Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday)\s+(\d{1,2})(?:st|nd|rd|th)?\s+([A-Z][a-z]+),?\s+(\d{4})', text)
    if match:
        try:
            day, month_str, year = match.group(1), match.group(2), match.group(3)
            dt = datetime.datetime.strptime(f"{day} {month_str} {year}", "%d %B %Y")
            return validate_year(dt.strftime("%Y-%m-%d"))
        except ValueError:
            pass
    
    return None

def determine_sermon_date(title, description, yt_obj):
    # Priority 1: Check for "Streamed live on" date in description (most reliable for livestreams/tape services)
    streamed_date = extract_streamed_live_date(description)
    if streamed_date: return streamed_date
    
    # Priority 2: Extract date from title
    date = extract_date_from_text(title)
    if date: return date
    
    # Priority 3: Extract date from description (other date formats)
    date = extract_date_from_text(description)
    if date: return date
    
    # Priority 4: Fall back to YouTube publish date
    if yt_obj:
        try: return yt_obj.publish_date.strftime("%Y-%m-%d")
        except: pass
    return "Unknown Date"

def format_sermon_entry(video_id, title, date_str, transcript_text, church_name, speaker, language, video_type, description="", filename=None, duration_minutes=0):
    # Truncate description if too long (keep first 2000 chars)
    desc_text = description[:2000] + "..." if len(description) > 2000 else description
    desc_section = f"Description:\n{desc_text}\n" if desc_text.strip() else ""
    # Use provided filename or construct one (ensuring header matches actual filename)
    header_filename = filename if filename else f"{date_str} - {title} - {speaker}.txt"
    # Format duration as human-readable
    if duration_minutes > 0:
        hours = int(duration_minutes // 60)
        mins = int(duration_minutes % 60)
        if hours > 0:
            duration_str = f"{hours}h {mins}m"
        else:
            duration_str = f"{mins}m"
    else:
        duration_str = "Unknown"
    return (
        f"################################################################################\n"
        f"START OF FILE: {header_filename}\n"
        f"################################################################################\n\n"
        f"SERMON DETAILS\n"
        f"========================================\n"
        f"Date:     {date_str}\n"
        f"Title:    {title}\n"
        f"Speaker:  {speaker}\n"
        f"Church:   {church_name}\n"
        f"Type:     {video_type}\n"
        f"Duration: {duration_str}\n"
        f"Language: {language}\n"
        f"URL:      https://www.youtube.com/watch?v={video_id}\n"
        f"========================================\n\n"
        f"{desc_section}"
        f"TRANSCRIPT\n"
        f"========================================\n"
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

def xml_to_timestamped_segments(xml_content):
    """
    Parse YouTube caption XML into timestamped segments.
    Returns a list of dicts: [{"start": float, "dur": float, "text": str}, ...]
    """
    try:
        root = ET.fromstring(xml_content)
        segments = []
        for child in root:
            if child.tag == 'text':
                start = float(child.get('start', 0))
                dur = float(child.get('dur', 0))
                text = child.text or ""
                # Clean up HTML entities
                text = text.replace('&nbsp;', ' ').replace('&#39;', "'").replace('&quot;', '"').replace('&amp;', '&')
                text = " ".join(text.split())
                if text:  # Only include non-empty segments
                    segments.append({
                        "start": round(start, 2),
                        "dur": round(dur, 2),
                        "text": text
                    })
        return segments
    except Exception as e:
        print(f"      ⚠️ Error parsing timestamped segments: {e}")
        return None

def format_timestamp(seconds):
    """Convert seconds to human-readable timestamp [H:MM:SS] or [M:SS]."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hours > 0:
        return f"[{hours}:{minutes:02d}:{secs:02d}]"
    else:
        return f"[{minutes}:{secs:02d}]"

def segments_to_timestamped_text(segments):
    """
    Convert timestamp segments to human-readable timestamped text.
    Each line starts with a timestamp like [0:00] or [1:23:45]
    """
    if not segments:
        return None
    
    lines = []
    for seg in segments:
        timestamp = format_timestamp(seg['start'])
        text = seg['text']
        lines.append(f"{timestamp} {text}")
    
    return "\n".join(lines)

def save_timestamp_data(filepath, video_id, segments):
    """
    Save timestamped transcript as a .timestamped.txt file alongside the plain .txt file.
    filepath: Path to the .txt file (will replace .txt with .timestamped.txt)
    """
    if not segments:
        return False
    
    timestamped_path = filepath.replace('.txt', '.timestamped.txt')
    
    try:
        # Convert segments to timestamped text
        timestamped_text = segments_to_timestamped_text(segments)
        if not timestamped_text:
            return False
        
        # Build header similar to regular transcript
        # Extract info from filepath
        filename = os.path.basename(filepath)
        timestamped_filename = filename.replace('.txt', '.timestamped.txt')
        
        header = (
            f"################################################################################\n"
            f"TIMESTAMPED TRANSCRIPT\n"
            f"################################################################################\n"
            f"Source: {filename}\n"
            f"Video ID: {video_id}\n"
            f"Segments: {len(segments)}\n"
            f"URL: https://www.youtube.com/watch?v={video_id}\n"
            f"================================================================================\n"
            f"Click any timestamp to jump to that point in the video.\n"
            f"Format: [M:SS] or [H:MM:SS] followed by transcript text.\n"
            f"================================================================================\n\n"
        )
        
        with open(timestamped_path, 'w', encoding='utf-8') as f:
            f.write(header)
            f.write(timestamped_text)
            f.write("\n")
        
        return True
    except Exception as e:
        print(f"      ⚠️ Error saving timestamped transcript: {e}")
        return False

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
    """
    Fetch transcript data for a YouTube video.
    Returns: (plain_text, description, yt_obj, raw_xml)
    raw_xml can be used to extract timestamped segments.
    """
    caption_track = None
    yt_obj = None
    try: caption_track, yt_obj = fetch_captions_with_client(video_id, 'WEB')
    except: pass
    if not caption_track:
        try: caption_track, yt_obj = fetch_captions_with_client(video_id, 'ANDROID')
        except: pass
    if not yt_obj: return None, "", None, None
    description = ""
    try: description = yt_obj.description or ""
    except: pass
    if not caption_track: return None, description, yt_obj, None
    try:
        response = requests.get(caption_track.url, headers=get_random_headers())
        if response.status_code == 200:
            raw_xml = response.text
            clean_text = xml_to_text(raw_xml)
            return clean_text, description, yt_obj, raw_xml
        else: return None, description, yt_obj, None
    except: return None, description, yt_obj, None

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

def metadata_only_scan(church_name, config, known_speakers):
    """
    Scans a YouTube channel for ALL videos and collects metadata (date, title, description)
    WITHOUT downloading transcripts. Useful for quickly cataloging all videos.
    PRESERVES all existing entries - never deletes previous data.
    Also processes unlisted URLs from existing summary CSV.
    """
    channel_url = config['url']
    clean_channel_name = church_name.replace(' ', '_')
    channel_dir = os.path.join(DATA_DIR, clean_channel_name)
    os.makedirs(channel_dir, exist_ok=True)

    print(f"\n" + "="*60)
    print(f"METADATA-ONLY SCAN: {church_name}")
    print(f"="*60)
    print(f"   🔍 Scanning for ALL videos (no transcript download)...")
    
    # Load existing history - NEVER DELETE existing entries
    existing_history = load_summary_history_csv(church_name)
    print(f"   📂 Loaded {len(existing_history)} existing entries from summary.")
    
    base_channel_url = channel_url.split('/streams')[0].split('/videos')[0].split('/featured')[0] if channel_url else None
    all_videos = []
    
    # Only scrape YouTube channel if URL is provided
    if base_channel_url:
        try:
            print(f"   🌐 Fetching video list from YouTube (no limit)...")
            all_videos.extend(list(scrapetube.get_channel(channel_url=base_channel_url, content_type='streams', limit=None)))
            all_videos.extend(list(scrapetube.get_channel(channel_url=base_channel_url, content_type='videos', limit=None)))
        except Exception as e:
            print(f"   ⚠️ Scrape Error: {e}")
    else:
        print(f"   ℹ️ No channel URL - scanning playlists only...")
    
    # Also fetch videos from configured playlists (e.g., legacy/archived content)
    playlists = config.get('playlists', [])
    for playlist_info in playlists:
        playlist_id = playlist_info.get('id')
        playlist_name = playlist_info.get('name', playlist_id)
        if playlist_id:
            print(f"   📋 Scanning playlist: {playlist_name}...")
            playlist_videos = fetch_playlist_videos(playlist_id, limit=None)
            print(f"      Found {len(playlist_videos)} videos in playlist.")
            all_videos.extend(playlist_videos)
    
    # Deduplicate channel videos
    unique_videos_map = {v['videoId']: v for v in all_videos}
    
    # CRITICAL: Also include unlisted URLs from existing summary CSV
    # These may be videos discovered via supplemental scrapers (e.g., church website)
    unlisted_count = 0
    for url, entry in existing_history.items():
        if url and 'watch?v=' in url:
            video_id = url.split('watch?v=')[-1].split('&')[0]
            if video_id not in unique_videos_map:
                # This is an unlisted video - add it to the processing queue
                unique_videos_map[video_id] = {
                    'videoId': video_id,
                    'title': {'runs': [{'text': entry.get('title', 'Unknown Title')}]},
                    '_from_summary': True  # Flag to identify unlisted videos
                }
                unlisted_count += 1
    
    unique_videos = list(unique_videos_map.values())
    print(f"   📊 Found {len(unique_videos) - unlisted_count} videos on channel + {unlisted_count} unlisted from summary.")
    
    if len(unique_videos) == 0:
        print("   ⚠️ No videos found from YouTube scan.")
        # ARCHIVAL RULE: Never delete historical entries just because we couldn't find videos.
        if existing_history:
            print(f"   📦 PRESERVING {len(existing_history)} historical entries (archival mode).")
        return
    
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    new_count = 0
    updated_count = 0
    unlisted_updated = 0
    
    for i, video in enumerate(unique_videos, 1):
        video_id = video['videoId']
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        is_unlisted = video.get('_from_summary', False)
        
        try:
            title = video['title']['runs'][0]['text']
        except:
            title = "Unknown Title"
        
        # Check if already exists in history
        if video_url in existing_history:
            existing = existing_history[video_url]
            existing_status = existing.get('status', '')
            
            # For unlisted videos or those with incomplete metadata, try to refresh
            if is_unlisted and existing_status in ['Metadata Only', 'Metadata Error', 'No Transcript']:
                print(f"   [{i}/{len(unique_videos)}] UNLISTED REFRESH: {title[:50]}...")
                try:
                    time.sleep(1)  # Rate limiting
                    yt_obj = YouTube(video_url, use_oauth=False, allow_oauth_cache=True)
                    description = yt_obj.description or ""
                    
                    # Update metadata if we got better info
                    if description and not existing.get('description'):
                        existing['description'] = description.replace('\n', ' ').replace('\r', ' ')[:500]
                    if existing.get('date') == 'Unknown Date':
                        existing['date'] = determine_sermon_date(title, description, yt_obj)
                    if existing.get('speaker') == 'Unknown Speaker':
                        speaker, _ = identify_speaker_dynamic(title, description, known_speakers)
                        speaker = normalize_speaker(speaker)
                        speaker = clean_name(speaker)
                        existing['speaker'] = speaker
                    
                    existing['last_checked'] = today_str
                    unlisted_updated += 1
                except Exception as e:
                    print(f"      ⚠️ Could not refresh unlisted video: {str(e)[:40]}")
                continue
            
            # For regular videos, just update title if changed
            if existing.get('title') != title:
                existing['title'] = title
                existing['last_checked'] = today_str
                updated_count += 1
            continue
        
        # New video - fetch metadata from YouTube
        print(f"   [{i}/{len(unique_videos)}] NEW: {title[:60]}...")
        
        try:
            time.sleep(1)  # Rate limiting
            yt_obj = YouTube(video_url, use_oauth=False, allow_oauth_cache=True)
            description = yt_obj.description or ""
            sermon_date = determine_sermon_date(title, description, yt_obj)
            
            # Identify speaker from title and description
            speaker, _ = identify_speaker_dynamic(title, description, known_speakers)
            speaker = normalize_speaker(speaker)
            speaker = clean_name(speaker)
            
            # Get duration for video type detection
            duration_minutes = 0
            try:
                duration_seconds = yt_obj.length if hasattr(yt_obj, 'length') else 0
                duration_minutes = duration_seconds / 60 if duration_seconds else 0
            except:
                pass
            
            video_type = determine_video_type(title, speaker, None, yt_obj, description)
            if video_type == "Memorial Service" and speaker != "William M. Branham":
                speaker = "Unknown Speaker"
            
            # Sanitize description for CSV
            desc_for_csv = description.replace('\n', ' ').replace('\r', ' ')[:500] if description else ""
            
            # Add to history (status = "Metadata Only" to indicate no transcript)
            existing_history[video_url] = {
                "date": sermon_date,
                "status": "Metadata Only",
                "speaker": speaker,
                "title": title,
                "url": video_url,
                "last_checked": today_str,
                "language": "Unknown",
                "type": video_type,
                "description": desc_for_csv,
                "duration": int(duration_minutes) if duration_minutes else 0,
                "church": church_name
            }
            new_count += 1
            
        except Exception as e:
            print(f"      ⚠️ Error fetching metadata: {str(e)[:50]}")
            # Still add with minimal info
            existing_history[video_url] = {
                "date": "Unknown Date",
                "status": "Metadata Error",
                "speaker": "Unknown Speaker",
                "title": title,
                "url": video_url,
                "last_checked": today_str,
                "language": "Unknown",
                "type": "Unknown",
                "description": "",
                "duration": 0,
                "church": church_name
            }
            new_count += 1
    
    # Write merged summary CSV
    csv_path = get_summary_file_path(church_name, ".csv")
    summary_list = list(existing_history.values())
    
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["date", "status", "speaker", "title", "url", "last_checked", "language", "type", "description", "duration", "church"])
            writer.writeheader()
            writer.writerows(summary_list)
        print(f"\n   ✅ SCAN COMPLETE")
        print(f"      New videos found: {new_count}")
        print(f"      Titles updated: {updated_count}")
        print(f"      Unlisted refreshed: {unlisted_updated}")
        print(f"      Total entries: {len(summary_list)}")
    except Exception as e:
        print(f"   ❌ Error writing summary CSV: {e}")

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
    """
    Clean up bad speaker names in history entries.
    
    ARCHIVAL RULE: We NEVER delete entries from history. If a speaker name is
    invalid, we reset it to "Unknown Speaker" but preserve all other data.
    This ensures we never lose scraped transcripts or metadata.
    """
    cleaned_history = {}
    fixed_count = 0
    for url, entry in history.items():
        speaker = entry.get('speaker', '')
        # Basic check - "Brother X" or "Sister X" patterns are kept as-is
        if "Brother " in speaker or "Sister " in speaker:
            cleaned_history[url] = entry
        elif not is_valid_person_name(speaker):
            # ARCHIVAL: Don't delete! Just reset the speaker to Unknown
            fixed_entry = entry.copy()
            fixed_entry['speaker'] = 'Unknown Speaker'
            cleaned_history[url] = fixed_entry
            fixed_count += 1
        else:
            cleaned_history[url] = entry
    if fixed_count > 0:
        print(f"   🧹 Reset {fixed_count} invalid speaker names to 'Unknown Speaker' (entries preserved).")
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

def process_channel(church_name, config, known_speakers, limit=None, recent_only=False, days_back=None):
    """
    Process a YouTube channel for sermon transcripts.
    
    Args:
        church_name: Name of the church/channel
        config: Channel configuration dict
        known_speakers: Set of known speaker names
        limit: Max number of videos to scan (None = no limit)
        recent_only: Legacy flag for 24-hour mode (deprecated, use days_back=1)
        days_back: Number of days to look back (None = full archive, 7 = last week)
    
    Returns:
        dict: Statistics about speaker detection for this channel
    """
    # Initialize stats tracking
    channel_stats = {
        'total_processed': 0,
        'speakers_detected': 0,
        'unknown_speakers': 0,
        'new_speakers': set()
    }
    
    channel_url = config['url']
    clean_channel_name = church_name.replace(' ', '_')
    channel_dir = os.path.join(DATA_DIR, clean_channel_name)
    os.makedirs(channel_dir, exist_ok=True)

    print(f"\n--------------------------------------------------")
    print(f"Processing Channel: {church_name}")
    if limit:
        print(f"   📊 Scan Limit: Most recent {limit} videos.")
    elif days_back:
        print(f"   📊 Scan Limit: Videos from the last {days_back} days.")
    elif recent_only:
        print(f"   📊 Scan Limit: Videos in the last 24 hours.")
        days_back = 1  # Convert legacy flag to days_back
    else:
        print(f"   📊 Scan Limit: FULL ARCHIVE.")
    time.sleep(1)

    history = load_summary_history_csv(church_name)
    history = clean_history_of_bad_speakers(history)

    base_channel_url = channel_url.split('/streams')[0].split('/videos')[0].split('/featured')[0] if channel_url else None
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
        
        # Only scrape YouTube channel if URL is provided
        if base_channel_url:
            try:
                print(f"   🔍 Scanning YouTube for videos...")
                all_videos.extend(list(scrapetube.get_channel(channel_url=base_channel_url, content_type='streams', limit=limit)))
                all_videos.extend(list(scrapetube.get_channel(channel_url=base_channel_url, content_type='videos', limit=limit)))
            except Exception as e:
                print(f"   ⚠️ Scrape Error: {e}")
        else:
            print(f"   ℹ️ No channel URL - scanning playlists only...")
        
        # Also fetch videos from configured playlists (e.g., legacy/archived content)
        playlists = config.get('playlists', [])
        for playlist_info in playlists:
            playlist_id = playlist_info.get('id')
            playlist_name = playlist_info.get('name', playlist_id)
            if playlist_id:
                print(f"   📋 Scanning playlist: {playlist_name}...")
                playlist_videos = fetch_playlist_videos(playlist_id, limit=limit)
                print(f"      Found {len(playlist_videos)} videos in playlist.")
                all_videos.extend(playlist_videos)

    unique_videos_map = {}
    for v in all_videos:
        if 'manual_date' not in v: unique_videos_map[v['videoId']] = v
    for v in all_videos:
        if 'manual_date' in v: unique_videos_map[v['videoId']] = v

    # CRITICAL: Also include unlisted URLs from summary CSV history
    # These may be videos discovered via supplemental scrapers (e.g., church website)
    unlisted_from_history = 0
    for url, entry in history.items():
        if url and 'watch?v=' in url:
            video_id = url.split('watch?v=')[-1].split('&')[0]
            if video_id not in unique_videos_map:
                # This is an unlisted video from history - add to processing queue
                unique_videos_map[video_id] = {
                    'videoId': video_id,
                    'title': {'runs': [{'text': entry.get('title', 'Unknown Title')}]},
                    'manual_date': entry.get('date') if entry.get('date') != 'Unknown Date' else None,
                    'manual_speaker': entry.get('speaker') if entry.get('speaker') != 'Unknown Speaker' else None,
                    '_from_history': True  # Flag to identify videos from history
                }
                unlisted_from_history += 1
    
    if unlisted_from_history > 0:
        print(f"   📋 Added {unlisted_from_history} unlisted videos from summary CSV.")

    unique_videos = unique_videos_map.values()
    print(f"   Videos found: {len(unique_videos)}")
    
    if len(unique_videos) == 0:
        print("   ⚠️ No videos found from YouTube scan.")
        # ARCHIVAL RULE: Never delete historical entries just because we couldn't find videos.
        # The channel may be temporarily unavailable, or videos may have been removed.
        # We preserve all existing data for archival purposes.
        if history:
            print(f"   📦 PRESERVING {len(history)} historical entries (archival mode).")
        return channel_stats  # Return empty stats but don't touch the existing CSV

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
        
        # Check if the video is recent enough to be processed (when using days_back filter)
        if days_back and church_name != "Shalom Tabernacle" and church_name != "Shalom Tabernacle Tucson":
            published_time_text = video.get('publishedTimeText', {}).get('simpleText', '')
            if not parse_published_time(published_time_text, max_days=days_back):
                continue # Skip if the video is older than days_back

        manual_date = video.get('manual_date')
        manual_speaker = video.get('manual_speaker')

        if manual_speaker:
            speaker = normalize_speaker(manual_speaker)
            if speaker not in known_speakers:
                known_speakers.add(speaker)
                channel_stats['new_speakers'].add(speaker)
                save_json_file(SPEAKERS_FILE, known_speakers)
        else:
            speaker, is_new = identify_speaker_dynamic(title, "", known_speakers)
            speaker = normalize_speaker(speaker)
            speaker = clean_name(speaker) 
            if is_new:
                print(f"   🎉 LEARNED NEW SPEAKER: {speaker}")
                channel_stats['new_speakers'].add(speaker)
                save_json_file(SPEAKERS_FILE, known_speakers)

        video_type = determine_video_type(title, speaker)
        if video_type == "Memorial Service" and speaker != "William M. Branham":
            speaker = "Unknown Speaker"
        
        # Track speaker detection stats
        channel_stats['total_processed'] += 1
        if speaker and speaker != "Unknown Speaker":
            channel_stats['speakers_detected'] += 1
        else:
            channel_stats['unknown_speakers'] += 1

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
            time.sleep(1)  # Rate limiting
            transcript_text, description, yt_obj, raw_xml = get_transcript_data(video_id)
            if manual_date: sermon_date = manual_date
            else: sermon_date = determine_sermon_date(title, description, yt_obj)
            language = determine_language(title, yt_obj)
            
            # Get video duration
            duration_minutes = 0
            if yt_obj:
                try:
                    duration_seconds = yt_obj.length if hasattr(yt_obj, 'length') else 0
                    duration_minutes = duration_seconds / 60 if duration_seconds else 0
                except:
                    pass
            
            if speaker == "Unknown Speaker":
                speaker, _ = identify_speaker_dynamic(title, description, known_speakers)
                speaker = normalize_speaker(speaker)
                speaker = clean_name(speaker)
            
            video_type = determine_video_type(title, speaker, transcript_text, yt_obj, description)
            if video_type == "Memorial Service" and speaker != "William M. Branham":
                speaker = "Unknown Speaker"
            
            # Print speaker and category identification
            duration_str = f"{int(duration_minutes)}m" if duration_minutes > 0 else "?"
            print(f"   👤 Speaker: {speaker} | 📂 Type: {video_type} | ⏱️ {duration_str}")

            status = "Success"
            # Sanitize description for CSV (remove newlines, limit length)
            desc_for_csv = description.replace('\n', ' ').replace('\r', ' ')[:500] if description else ""
            
            if not transcript_text:
                status = "No Transcript"
                print(f"   ❌ No Transcript found (Lang: {language}).")
            else:
                safe_title = sanitize_filename(title)
                safe_speaker = sanitize_filename(speaker)
                filename = f"{sermon_date} - {safe_title} - {safe_speaker}.txt"
                filepath = os.path.join(channel_dir, filename)
                if not os.path.exists(filepath):
                    entry = format_sermon_entry(video_id, title, sermon_date, transcript_text, church_name, speaker, language, video_type, description, filename=filename, duration_minutes=duration_minutes)
                    with open(filepath, 'a', encoding='utf-8') as f: f.write(entry)
                    print(f"   ✅ Transcript downloaded & Saved (Lang: {language}).")
                else:
                    entry = format_sermon_entry(video_id, title, sermon_date, transcript_text, church_name, speaker, language, video_type, description, filename=filename, duration_minutes=duration_minutes)
                    with open(filepath, 'w', encoding='utf-8') as f: f.write(entry)
                    print(f"   ✅ File updated.")
                
                # Save timestamped segments if available
                if raw_xml:
                    segments = xml_to_timestamped_segments(raw_xml)
                    if segments and save_timestamp_data(filepath, video_id, segments):
                        print(f"   📍 Timestamps saved ({len(segments)} segments).")

            current_summary_list.append({
                "date": sermon_date, "status": status, "speaker": speaker,
                "title": title, "url": video_url, "last_checked": today_str,
                "language": language, "type": video_type, "description": desc_for_csv,
                "duration": int(duration_minutes) if duration_minutes else 0,
                "church": church_name
            })
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
            current_summary_list.append({
                "date": "Error", "status": "Failed", "speaker": "Unknown",
                "title": title, "url": video_url, "last_checked": today_str,
                "language": "Unknown", "type": "Unknown", "description": "",
                "duration": 0, "church": church_name
            })

    csv_path = get_summary_file_path(church_name, ".csv")
    # --- NEW LOGIC: Ensure every .txt transcript is represented in the summary CSV ---
    channel_dir = os.path.join(DATA_DIR, church_name.replace(' ', '_'))
    txt_files = [f for f in os.listdir(channel_dir) if f.endswith('.txt')]
    
    # Build a set of URLs from current_summary_list for fast lookup
    processed_urls = set()
    for entry in current_summary_list:
        if entry.get('url'):
            processed_urls.add(entry.get('url'))
    
    # =========================================================================
    # ARCHIVAL RULE: NEVER DELETE HISTORICAL ENTRIES
    # =========================================================================
    # Even if a video URL is no longer found on the channel (deleted, unlisted,
    # or made private), we ALWAYS preserve existing entries from our history.
    # This ensures we maintain our scraped transcripts and metadata for archival
    # purposes. A video disappearing from YouTube doesn't mean we lose our data.
    # =========================================================================
    preserved_count = 0
    for url, hist_entry in history.items():
        if url not in processed_urls:
            # This entry exists in history but wasn't found in this scan - PRESERVE IT
            current_summary_list.append(hist_entry)
            processed_urls.add(url)
            preserved_count += 1
    
    if preserved_count > 0:
        print(f"   📦 Preserved {preserved_count} historical entries not found in current scan.")
    
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
                "type": video_type,
                "description": ""  # Empty for legacy files
            })
            summary_keys.add((date, title, speaker))
    
    # --- WOLJC POST-PROCESSING: Update speakers for Word of Life Church ---
    if church_name == "Word Of Life Church" and woljc_update_speakers:
        # Find entries with Unknown Speaker that were just processed
        unknown_entries = [e for e in current_summary_list 
                          if e.get('speaker') == "Unknown Speaker" and e.get('date')]
        if unknown_entries:
            try:
                woljc_result = woljc_update_speakers(unknown_entries, dry_run=False)
                if woljc_result.get('updated', 0) > 0:
                    # Also rename any transcript files that were updated
                    channel_dir = os.path.join(DATA_DIR, church_name.replace(' ', '_'))
                    for match in woljc_result.get('matches', []):
                        old_speaker = match['old_speaker']
                        new_speaker = match['new_speaker']
                        date = match['date']
                        title = match['title']
                        # Find and rename the transcript file
                        for filename in os.listdir(channel_dir):
                            if filename.endswith('.txt') and filename.startswith(date):
                                if old_speaker in filename or 'Unknown Speaker' in filename:
                                    safe_title = sanitize_filename(title)
                                    safe_speaker = sanitize_filename(new_speaker)
                                    new_filename = f"{date} - {safe_title} - {safe_speaker}.txt"
                                    old_path = os.path.join(channel_dir, filename)
                                    new_path = os.path.join(channel_dir, new_filename)
                                    if old_path != new_path and os.path.exists(old_path):
                                        try:
                                            # CRITICAL: Update internal Speaker: header BEFORE renaming
                                            update_transcript_speaker_header(old_path, new_speaker)
                                            os.rename(old_path, new_path)
                                        except OSError:
                                            pass
                                    break
            except Exception as e:
                print(f"   ⚠️ WOLJC speaker update error: {e}")
    
    # Deduplicate by URL before writing (keep most recent entry based on last_checked)
    url_to_entry = {}
    for entry in current_summary_list:
        url = entry.get('url', '')
        if url:
            existing = url_to_entry.get(url)
            if existing:
                # Keep the entry with the more recent last_checked date
                existing_date = existing.get('last_checked', '1900-01-01')
                new_date = entry.get('last_checked', '1900-01-01')
                if new_date >= existing_date:
                    url_to_entry[url] = entry
            else:
                url_to_entry[url] = entry
        else:
            # Entries without URL - use (date, title, speaker) as key
            key = (entry.get('date', ''), entry.get('title', ''), entry.get('speaker', ''))
            url_to_entry[str(key)] = entry
    
    final_summary_list = list(url_to_entry.values())
    
    # Write the updated summary CSV
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["date", "status", "speaker", "title", "url", "last_checked", "language", "type", "description", "duration", "church"])
            writer.writeheader()
            writer.writerows(final_summary_list)
    except Exception as e:
        print(f"   ❌ Error writing summary CSV: {e}")
    
    save_json_file(SPEAKERS_FILE, known_speakers)
    print(f"SUCCESS: {church_name} complete.")
    
    return channel_stats


def list_pending_videos(data_dir, churches=None, include_no_transcript=False):
    """
    List all videos that don't have transcripts yet (Metadata Only and optionally No Transcript).
    
    Args:
        data_dir: Directory containing the Summary CSV files
        churches: Optional list of specific churches to filter by
        include_no_transcript: If True, also include 'No Transcript' videos
    """
    statuses = ['Metadata Only']
    if include_no_transcript:
        statuses.append('No Transcript')
    
    status_label = "Metadata Only" + (" + No Transcript" if include_no_transcript else "")
    
    print("\n" + "=" * 70)
    print(f"LISTING VIDEOS: {status_label}")
    print("=" * 70)
    
    # Find all Summary CSV files
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('_Summary.csv')]
    
    grand_total = 0
    
    for csv_file in sorted(csv_files):
        church_name = csv_file.replace('_Summary.csv', '').replace('_', ' ')
        
        # Filter by church if specified
        if churches:
            if not any(c.lower() in church_name.lower() for c in churches):
                continue
        
        csv_path = os.path.join(data_dir, csv_file)
        
        # Load CSV
        rows = []
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(row)
        except Exception as e:
            continue
        
        # Find matching entries
        pending = [r for r in rows if r.get('status') in statuses]
        
        if not pending:
            continue
        
        print(f"\n📂 {church_name}: {len(pending)} videos")
        print("-" * 60)
        
        # Group by status
        metadata_only = [r for r in pending if r.get('status') == 'Metadata Only']
        no_transcript = [r for r in pending if r.get('status') == 'No Transcript']
        
        if metadata_only:
            print(f"  Metadata Only ({len(metadata_only)}):")
            for entry in metadata_only:
                date = entry.get('date', 'Unknown')
                title = entry.get('title', 'Unknown')[:50]
                speaker = entry.get('speaker', 'Unknown')
                print(f"    {date} | {speaker[:20]:<20} | {title}")
        
        if no_transcript:
            print(f"  No Transcript ({len(no_transcript)}):")
            for entry in no_transcript:
                date = entry.get('date', 'Unknown')
                title = entry.get('title', 'Unknown')[:50]
                speaker = entry.get('speaker', 'Unknown')
                print(f"    {date} | {speaker[:20]:<20} | {title}")
        
        grand_total += len(pending)
    
    print("\n" + "=" * 70)
    print(f"TOTAL: {grand_total} videos without transcripts")
    print("=" * 70)


def retry_metadata_only_videos(data_dir, churches=None, limit=None, include_no_transcript=False):
    """
    Retry fetching transcripts for videos that currently have 'Metadata Only' status.
    
    Args:
        data_dir: Directory containing the Summary CSV files
        churches: Optional list of specific churches to process (matches partial names)
        limit: Maximum number of videos to retry per church
        include_no_transcript: If True, also retry 'No Transcript' videos
    """
    statuses = ['Metadata Only']
    if include_no_transcript:
        statuses.append('No Transcript')
    
    status_label = "Metadata Only" + (" + No Transcript" if include_no_transcript else "")
    
    print("\n" + "=" * 70)
    print(f"RETRY VIDEOS: {status_label}")
    print("=" * 70)
    
    # Load known speakers
    known_speakers = load_json_file(SPEAKERS_FILE)
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    
    # Find all Summary CSV files
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('_Summary.csv')]
    
    total_retried = 0
    total_success = 0
    total_failed = 0
    
    for csv_file in sorted(csv_files):
        church_name = csv_file.replace('_Summary.csv', '').replace('_', ' ')
        
        # Filter by church if specified
        if churches:
            if not any(c.lower() in church_name.lower() for c in churches):
                continue
        
        csv_path = os.path.join(data_dir, csv_file)
        church_dir = os.path.join(data_dir, csv_file.replace('_Summary.csv', ''))
        
        # Load CSV
        rows = []
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(row)
        except Exception as e:
            print(f"   ⚠️ Error reading {csv_file}: {e}")
            continue
        
        # Find entries to retry
        to_retry = [r for r in rows if r.get('status') in statuses]
        
        if not to_retry:
            continue
        
        print(f"\n📂 {church_name}: {len(to_retry)} entries to retry")
        
        # Apply limit if specified
        if limit:
            to_retry = to_retry[:limit]
            print(f"   (Limited to {limit} entries)")
        
        retried = 0
        success = 0
        failed = 0
        
        for entry in to_retry:
            url = entry.get('url', '')
            title = entry.get('title', 'Unknown')
            
            if not url or 'youtube.com' not in url:
                continue
            
            # Extract video ID
            if 'v=' in url:
                video_id = url.split('v=')[1].split('&')[0]
            else:
                video_id = url.split('/')[-1].split('?')[0]
            
            print(f"   [{retried + 1}] Retrying: {title[:50]}...")
            retried += 1
            
            try:
                time.sleep(1)  # Rate limiting
                transcript_text, description, yt_obj, raw_xml = get_transcript_data(video_id)
                
                if not transcript_text:
                    print(f"       ❌ Still no transcript available")
                    failed += 1
                    entry['status'] = 'No Transcript'
                    entry['last_checked'] = today_str
                    continue
                
                # Got a transcript! Update the entry
                speaker = entry.get('speaker', 'Unknown Speaker')
                sermon_date = entry.get('date', 'Unknown Date')
                language = determine_language(title, yt_obj) if yt_obj else 'Unknown'
                video_type = entry.get('type', 'Full Sermon')
                
                # Update speaker if currently unknown
                if speaker == 'Unknown Speaker' and description:
                    new_speaker, _ = identify_speaker_dynamic(title, description, known_speakers)
                    new_speaker = normalize_speaker(new_speaker)
                    new_speaker = clean_name(new_speaker)
                    if new_speaker != 'Unknown Speaker':
                        speaker = new_speaker
                
                # Update date if unknown
                if sermon_date == 'Unknown Date' and yt_obj:
                    sermon_date = determine_sermon_date(title, description, yt_obj)
                
                # Create transcript file
                if not os.path.exists(church_dir):
                    os.makedirs(church_dir)
                
                safe_title = sanitize_filename(title)
                safe_speaker = sanitize_filename(speaker)
                filename = f"{sermon_date} - {safe_title} - {safe_speaker}.txt"
                filepath = os.path.join(church_dir, filename)
                
                if not os.path.exists(filepath):
                    content = format_sermon_entry(
                        video_id, title, sermon_date, transcript_text,
                        church_name, speaker, language, video_type, description, filename=filename
                    )
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(content)
                    print(f"       ✅ Transcript saved: {filename[:50]}...")
                    
                    # Save timestamped segments if available
                    if raw_xml:
                        segments = xml_to_timestamped_segments(raw_xml)
                        if segments and save_timestamp_data(filepath, video_id, segments):
                            print(f"       📍 Timestamps saved ({len(segments)} segments).")
                else:
                    print(f"       ✅ Transcript already exists")
                
                # Update CSV entry
                entry['status'] = 'Success'
                entry['speaker'] = speaker
                entry['date'] = sermon_date
                entry['language'] = language
                entry['last_checked'] = today_str
                if description:
                    entry['description'] = description.replace('\n', ' ').replace('\r', ' ')[:500]
                
                success += 1
                
            except Exception as e:
                print(f"       ⚠️ Error: {str(e)[:50]}")
                entry['last_checked'] = today_str
                failed += 1
        
        # Save updated CSV
        if retried > 0:
            try:
                with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                    fieldnames = ['date', 'status', 'speaker', 'title', 'url', 'last_checked', 'language', 'type', 'description', 'duration', 'church']
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
                print(f"   📝 Updated {csv_file}")
            except Exception as e:
                print(f"   ❌ Error saving CSV: {e}")
        
        total_retried += retried
        total_success += success
        total_failed += failed
        print(f"   Results: {success} success, {failed} failed")
    
    print("\n" + "=" * 70)
    print(f"RETRY COMPLETE: {total_retried} videos processed")
    print(f"  ✅ Success: {total_success}")
    print(f"  ❌ Failed:  {total_failed}")
    print("=" * 70)


def main():
    prevent_sleep()
    try:
        parser = argparse.ArgumentParser(description="Update sermon transcripts from YouTube channels.")
        parser.add_argument('--recent', action='store_true', help="Only process videos uploaded in the last 24 hours.")
        parser.add_argument('--days', type=int, default=None, help="Only process videos from the last N days (default: 7 for automation).")
        parser.add_argument('--heal', action='store_true', help="Only run the heal archive process.")
        parser.add_argument('--force', action='store_true', help="Force re-processing of all files during healing.")
        parser.add_argument('--backfill-descriptions', action='store_true', help="Backfill video descriptions into existing transcript files.")
        parser.add_argument('--backfill-timestamps', action='store_true', help="Backfill timestamped transcript data for files missing .timestamped.txt.")
        parser.add_argument('--backfill-duration', action='store_true', help="Backfill video duration metadata for entries missing it.")
        parser.add_argument('--migrate-church-names', action='store_true', help="Add church name column to all existing CSV summary files.")
        parser.add_argument('--heal-categories', action='store_true', help="Re-evaluate and fix video type categories based on title, description, and duration.")
        parser.add_argument('--dry-run', action='store_true', help="Show what would be done without making changes (for backfill operations).")
        parser.add_argument('--church', type=str, action='append', help="Specific church(es) to process (can be used multiple times).")
        parser.add_argument('--limit', type=int, default=None, help="Maximum number of files to process.")
        parser.add_argument('--unscraped', action='store_true', help="Only scrape channels that don't have a Summary CSV file yet.")
        parser.add_argument('--list-pending', action='store_true', help="List all videos without transcripts (Metadata Only).")
        parser.add_argument('--retry-metadata', action='store_true', help="Retry fetching transcripts for 'Metadata Only' videos.")
        parser.add_argument('--include-no-transcript', action='store_true', help="Include 'No Transcript' videos when listing or retrying.")
        parser.add_argument('--add-timestamps-video', type=str, metavar='URL', help="Add timestamps to a specific video by URL or video ID.")
        args = parser.parse_args()

        if args.add_timestamps_video:
            add_timestamps_for_video(args.add_timestamps_video, DATA_DIR)
            return

        if args.migrate_church_names:
            print("Migrating CSV files to add church names...")
            migrate_csv_add_church_names(DATA_DIR)
            return

        if args.backfill_duration:
            print("Backfilling video duration metadata...")
            churches_str = ','.join(args.church) if args.church else None
            backfill_duration_metadata(DATA_DIR, dry_run=args.dry_run, churches=churches_str, limit=args.limit)
            return

        if args.heal_categories:
            print("Healing video categories...")
            churches_str = ','.join(args.church) if args.church else None
            heal_video_categories(DATA_DIR, dry_run=args.dry_run, churches=churches_str)
            return

        if args.list_pending:
            list_pending_videos(DATA_DIR, churches=args.church, include_no_transcript=args.include_no_transcript)
            return

        if args.retry_metadata:
            status_label = "'Metadata Only'" + (" and 'No Transcript'" if args.include_no_transcript else "")
            print(f"Retrying transcript fetch for {status_label} videos...")
            retry_metadata_only_videos(DATA_DIR, churches=args.church, limit=args.limit, include_no_transcript=args.include_no_transcript)
            return

        if args.backfill_timestamps:
            print("Backfilling timestamps for transcript files...")
            backfill_timestamps(DATA_DIR, dry_run=args.dry_run, churches=args.church, limit=args.limit)
            return

        if args.backfill_descriptions:
            print("Backfilling video descriptions into transcript files...")
            backfill_descriptions(DATA_DIR, dry_run=args.dry_run, churches=args.church, limit=args.limit)
            return

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
        
        # When running with --days, process all channels for that time period
        if args.days:
            print(f"\\n🔄 PARTIAL SCRAPE: Last {args.days} days for ALL channels")
            print("="*50)
            all_stats = {'total_processed': 0, 'speakers_detected': 0, 'unknown_speakers': 0, 'by_church': {}, 'new_speakers': set()}
            for name, config in channels.items():
                channel_stats = process_channel(name, config, known_speakers, days_back=args.days)
                if channel_stats:
                    all_stats['total_processed'] += channel_stats.get('total_processed', 0)
                    all_stats['speakers_detected'] += channel_stats.get('speakers_detected', 0)
                    all_stats['unknown_speakers'] += channel_stats.get('unknown_speakers', 0)
                    all_stats['new_speakers'].update(channel_stats.get('new_speakers', set()))
                    if channel_stats.get('total_processed', 0) > 0:
                        all_stats['by_church'][name] = {
                            'total': channel_stats.get('total_processed', 0),
                            'detected': channel_stats.get('speakers_detected', 0),
                            'unknown': channel_stats.get('unknown_speakers', 0)
                        }
            write_speaker_detection_log(all_stats, operation_name=f"Partial Scrape (Last {args.days} Days)")
            print(f"\\n✅ Partial scrape ({args.days} days) complete. Running Post-Scrape Self-Healing...")
            heal_archive(DATA_DIR)
            return
        
        # When running with --recent, we don't need a menu.
        if args.recent:
            all_stats = {'total_processed': 0, 'speakers_detected': 0, 'unknown_speakers': 0, 'by_church': {}, 'new_speakers': set()}
            for name, config in channels.items():
                channel_stats = process_channel(name, config, known_speakers, days_back=1)
                if channel_stats:
                    all_stats['total_processed'] += channel_stats.get('total_processed', 0)
                    all_stats['speakers_detected'] += channel_stats.get('speakers_detected', 0)
                    all_stats['unknown_speakers'] += channel_stats.get('unknown_speakers', 0)
                    all_stats['new_speakers'].update(channel_stats.get('new_speakers', set()))
                    if channel_stats.get('total_processed', 0) > 0:
                        all_stats['by_church'][name] = {
                            'total': channel_stats.get('total_processed', 0),
                            'detected': channel_stats.get('speakers_detected', 0),
                            'unknown': channel_stats.get('unknown_speakers', 0)
                        }
            write_speaker_detection_log(all_stats, operation_name="Recent Scrape (Last 24 Hours)")
            print("\\n✅ Recent scrape complete. Running Post-Scrape Self-Healing...")
            heal_archive(DATA_DIR)
            return

        # When running with --unscraped, scrape only channels without Summary CSV
        if args.unscraped:
            print("\\n🔄 UNSCRAPED CHANNELS ONLY")
            print("="*50)
            
            # Find unscraped channels (no Summary CSV)
            unscraped_channels = {}
            for name, config in channels.items():
                normalized_name = name.replace(' ', '_')
                summary_path = os.path.join(DATA_DIR, f"{normalized_name}_Summary.csv")
                if not os.path.exists(summary_path):
                    unscraped_channels[name] = config
            
            if not unscraped_channels:
                print("✅ All channels have been scraped! No unscraped channels found.")
                return
            
            print(f"Found {len(unscraped_channels)} unscraped channel(s):")
            for name in unscraped_channels.keys():
                print(f"  • {name}")
            print()
            
            all_stats = {'total_processed': 0, 'speakers_detected': 0, 'unknown_speakers': 0, 'by_church': {}, 'new_speakers': set()}
            for name, config in unscraped_channels.items():
                print(f"\\n{'='*50}")
                print(f"SCRAPING: {name}")
                print(f"{'='*50}")
                channel_stats = process_channel(name, config, known_speakers)
                if channel_stats:
                    all_stats['total_processed'] += channel_stats.get('total_processed', 0)
                    all_stats['speakers_detected'] += channel_stats.get('speakers_detected', 0)
                    all_stats['unknown_speakers'] += channel_stats.get('unknown_speakers', 0)
                    all_stats['new_speakers'].update(channel_stats.get('new_speakers', set()))
                    if channel_stats.get('total_processed', 0) > 0:
                        all_stats['by_church'][name] = {
                            'total': channel_stats.get('total_processed', 0),
                            'detected': channel_stats.get('speakers_detected', 0),
                            'unknown': channel_stats.get('unknown_speakers', 0)
                        }
            
            write_speaker_detection_log(all_stats, operation_name="Unscraped Channels Scrape (CLI)")
            print(f"\\n✅ Unscraped channels scrape complete. Running Post-Scrape Self-Healing...")
            heal_archive(DATA_DIR)
            return

        # --- MENU ---
        print("\\n" + "="*50)
        print("ACTION SELECTION")
        print("="*50)
        print(" 1. Single Channel Scrape")
        print(" 2. All Channels Scrape")
        print(" 3. Run Deep Self-Healing & Cleanup (No Scraping)")
        print(" 4. Metadata-Only Scan (No Transcripts)")
        print(" 5. Partial Scrape (Last N Days)")
        print(" 6. Heal Speakers from CSV (speaker_detected)")
        print(" 7. Scrape Unscraped Channels Only (No Summary CSV)")
        print("="*50)
        
        action = input("\n👉 Enter Number: ").strip()
        
        if action == '3':
            heal_archive(DATA_DIR)
            return
        
        if action == '4':
            print("\n--- METADATA-ONLY SCAN ---")
            print("This scans ALL videos for metadata (date, title, description)")
            print("without downloading transcripts. Existing entries are preserved.\n")
            print("Available channels:")
            for i, name in enumerate(channels.keys(), 1):
                print(f"  {i}. {name}")
            print(f"  0. All Channels")
            choice = input("\n👉 Enter channel number (or 0 for all): ").strip()
            
            if choice == '0':
                for name, config in channels.items():
                    metadata_only_scan(name, config, known_speakers)
            else:
                try:
                    idx = int(choice) - 1
                    channel_names = list(channels.keys())
                    if 0 <= idx < len(channel_names):
                        name = channel_names[idx]
                        metadata_only_scan(name, channels[name], known_speakers)
                    else:
                        print("Invalid selection.")
                except ValueError:
                    print("Invalid input.")
            return
        
        if action == '5':
            print("\n--- PARTIAL SCRAPE (Last N Days) ---")
            print("This scrapes only videos uploaded within the specified number of days.")
            print("Existing transcripts are preserved. Only new videos are downloaded.\n")
            days_input = input("👉 Enter number of days to look back (default 7): ").strip()
            try:
                days_back = int(days_input) if days_input else 7
                if days_back < 1:
                    days_back = 7
            except ValueError:
                days_back = 7
            
            print(f"\nAvailable channels:")
            for i, name in enumerate(channels.keys(), 1):
                print(f"  {i}. {name}")
            print(f"  0. All Channels")
            choice = input("\n👉 Enter channel number (or 0 for all): ").strip()
            
            all_stats = {'total_processed': 0, 'speakers_detected': 0, 'unknown_speakers': 0, 'by_church': {}, 'new_speakers': set()}
            
            if choice == '0':
                print(f"\n🔄 Scanning ALL channels for videos from the last {days_back} days...\n")
                for name, config in channels.items():
                    channel_stats = process_channel(name, config, known_speakers, days_back=days_back)
                    if channel_stats:
                        all_stats['total_processed'] += channel_stats.get('total_processed', 0)
                        all_stats['speakers_detected'] += channel_stats.get('speakers_detected', 0)
                        all_stats['unknown_speakers'] += channel_stats.get('unknown_speakers', 0)
                        all_stats['new_speakers'].update(channel_stats.get('new_speakers', set()))
                        if channel_stats.get('total_processed', 0) > 0:
                            all_stats['by_church'][name] = {
                                'total': channel_stats.get('total_processed', 0),
                                'detected': channel_stats.get('speakers_detected', 0),
                                'unknown': channel_stats.get('unknown_speakers', 0)
                            }
            else:
                try:
                    idx = int(choice) - 1
                    channel_names = list(channels.keys())
                    if 0 <= idx < len(channel_names):
                        name = channel_names[idx]
                        print(f"\n🔄 Scanning {name} for videos from the last {days_back} days...\n")
                        channel_stats = process_channel(name, channels[name], known_speakers, days_back=days_back)
                        if channel_stats:
                            all_stats['total_processed'] += channel_stats.get('total_processed', 0)
                            all_stats['speakers_detected'] += channel_stats.get('speakers_detected', 0)
                            all_stats['unknown_speakers'] += channel_stats.get('unknown_speakers', 0)
                            all_stats['new_speakers'].update(channel_stats.get('new_speakers', set()))
                            if channel_stats.get('total_processed', 0) > 0:
                                all_stats['by_church'][name] = {
                                    'total': channel_stats.get('total_processed', 0),
                                    'detected': channel_stats.get('speakers_detected', 0),
                                    'unknown': channel_stats.get('unknown_speakers', 0)
                                }
                    else:
                        print("Invalid selection.")
                        return
                except ValueError:
                    print("Invalid input.")
                    return
            
            write_speaker_detection_log(all_stats, operation_name=f"Partial Scrape Menu (Last {days_back} Days)")
            print(f"\n✅ Partial scrape ({days_back} days) complete. Running Post-Scrape Self-Healing...")
            heal_archive(DATA_DIR)
            return
        
        if action == '6':
            print("\n--- HEAL SPEAKERS FROM CSV ---")
            print("This updates speaker names in transcript files and Summary CSVs")
            print("based on the 'speaker_detected' column from a corrected CSV file.\n")
            default_csv = "master_sermons_with_speaker_detected.csv"
            csv_input = input(f"👉 Enter CSV path (default: {default_csv}): ").strip()
            csv_path = csv_input if csv_input else default_csv
            heal_speakers_from_csv(csv_path)
            return

        if action == '7':
            print("\n--- SCRAPE UNSCRAPED CHANNELS ONLY ---")
            print("This scrapes only channels that don't have a Summary CSV file yet.")
            print("Useful for prioritizing new churches before doing a full re-scrape.\n")
            
            # Find unscraped channels (no Summary CSV)
            unscraped_channels = {}
            for name, config in channels.items():
                # Normalize church name to match file naming convention
                normalized_name = name.replace(' ', '_')
                summary_path = os.path.join(DATA_DIR, f"{normalized_name}_Summary.csv")
                if not os.path.exists(summary_path):
                    unscraped_channels[name] = config
            
            if not unscraped_channels:
                print("✅ All channels have been scraped! No unscraped channels found.")
                return
            
            print(f"Found {len(unscraped_channels)} unscraped channel(s):")
            for i, name in enumerate(unscraped_channels.keys(), 1):
                print(f"  {i}. {name}")
            print(f"  0. Scrape ALL unscraped channels")
            
            choice = input("\n👉 Enter channel number (or 0 for all): ").strip()
            
            all_stats = {'total_processed': 0, 'speakers_detected': 0, 'unknown_speakers': 0, 'by_church': {}, 'new_speakers': set()}
            
            if choice == '0':
                print(f"\n🔄 Scraping ALL {len(unscraped_channels)} unscraped channels...\n")
                for name, config in unscraped_channels.items():
                    print(f"\n{'='*50}")
                    print(f"SCRAPING: {name}")
                    print(f"{'='*50}")
                    channel_stats = process_channel(name, config, known_speakers)
                    if channel_stats:
                        all_stats['total_processed'] += channel_stats.get('total_processed', 0)
                        all_stats['speakers_detected'] += channel_stats.get('speakers_detected', 0)
                        all_stats['unknown_speakers'] += channel_stats.get('unknown_speakers', 0)
                        all_stats['new_speakers'].update(channel_stats.get('new_speakers', set()))
                        if channel_stats.get('total_processed', 0) > 0:
                            all_stats['by_church'][name] = {
                                'total': channel_stats.get('total_processed', 0),
                                'detected': channel_stats.get('speakers_detected', 0),
                                'unknown': channel_stats.get('unknown_speakers', 0)
                            }
            else:
                try:
                    idx = int(choice) - 1
                    channel_names = list(unscraped_channels.keys())
                    if 0 <= idx < len(channel_names):
                        name = channel_names[idx]
                        print(f"\n🔄 Scraping {name}...\n")
                        channel_stats = process_channel(name, unscraped_channels[name], known_speakers)
                        if channel_stats:
                            all_stats['total_processed'] += channel_stats.get('total_processed', 0)
                            all_stats['speakers_detected'] += channel_stats.get('speakers_detected', 0)
                            all_stats['unknown_speakers'] += channel_stats.get('unknown_speakers', 0)
                            all_stats['new_speakers'].update(channel_stats.get('new_speakers', set()))
                            if channel_stats.get('total_processed', 0) > 0:
                                all_stats['by_church'][name] = {
                                    'total': channel_stats.get('total_processed', 0),
                                    'detected': channel_stats.get('speakers_detected', 0),
                                    'unknown': channel_stats.get('unknown_speakers', 0)
                                }
                    else:
                        print("Invalid selection.")
                        return
                except ValueError:
                    print("Invalid input.")
                    return
            
            write_speaker_detection_log(all_stats, operation_name="Unscraped Channels Scrape")
            print(f"\n✅ Unscraped channels scrape complete. Running Post-Scrape Self-Healing...")
            heal_archive(DATA_DIR)
            return

        # --- CHANNEL SELECTION ---
        channel_name = ""
        if action == '1':
            print("\nAvailable channels:")
            channel_names = list(channels.keys())
            for i, name in enumerate(channel_names, 1):
                print(f"  {i}. {name}")
            choice = input("\n👉 Enter channel number: ").strip()
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(channel_names):
                    channel_name = channel_names[idx]
                    print(f"\n🔄 Scraping {channel_name}...\n")
                    channel_stats = process_channel(channel_name, channels[channel_name], known_speakers)
                    if channel_stats and channel_stats.get('total_processed', 0) > 0:
                        all_stats = {
                            'total_processed': channel_stats.get('total_processed', 0),
                            'speakers_detected': channel_stats.get('speakers_detected', 0),
                            'unknown_speakers': channel_stats.get('unknown_speakers', 0),
                            'new_speakers': channel_stats.get('new_speakers', set()),
                            'by_church': {channel_name: {
                                'total': channel_stats.get('total_processed', 0),
                                'detected': channel_stats.get('speakers_detected', 0),
                                'unknown': channel_stats.get('unknown_speakers', 0)
                            }}
                        }
                        write_speaker_detection_log(all_stats, operation_name=f"Single Channel Scrape: {channel_name}")
                else:
                    print("Invalid selection.")
            except ValueError:
                print("Invalid input.")
        elif action == '2':
            all_stats = {'total_processed': 0, 'speakers_detected': 0, 'unknown_speakers': 0, 'by_church': {}, 'new_speakers': set()}
            
            # Sort channels: unscraped first, then alphabetically
            def channel_sort_key(item):
                name, config = item
                summary_path = get_summary_file_path(name, ".csv")
                has_summary = os.path.exists(summary_path)
                # Return tuple: (0 if unscraped else 1, name) for sorting
                return (0 if not has_summary else 1, name.lower())
            
            sorted_channels = sorted(channels.items(), key=channel_sort_key)
            total_channels = len(sorted_channels)
            
            # Count unscraped for display
            unscraped_count = sum(1 for name, _ in sorted_channels if not os.path.exists(get_summary_file_path(name, ".csv")))
            if unscraped_count > 0:
                print(f"\n📋 Processing order: {unscraped_count} unscraped channel(s) first, then {total_channels - unscraped_count} existing channel(s)")
            
            for idx, (name, config) in enumerate(sorted_channels, 1):
                remaining = total_channels - idx
                print(f"\n{'='*60}")
                print(f"📺 CHANNEL {idx}/{total_channels}: {name}")
                print(f"{'='*60}")
                
                channel_stats = process_channel(name, config, known_speakers)
                if channel_stats:
                    all_stats['total_processed'] += channel_stats.get('total_processed', 0)
                    all_stats['speakers_detected'] += channel_stats.get('speakers_detected', 0)
                    all_stats['unknown_speakers'] += channel_stats.get('unknown_speakers', 0)
                    all_stats['new_speakers'].update(channel_stats.get('new_speakers', set()))
                    if channel_stats.get('total_processed', 0) > 0:
                        all_stats['by_church'][name] = {
                            'total': channel_stats.get('total_processed', 0),
                            'detected': channel_stats.get('speakers_detected', 0),
                            'unknown': channel_stats.get('unknown_speakers', 0)
                        }
                
                if remaining > 0:
                    print(f"\n✅ Finished {name}. {remaining} channel(s) remaining...")
                else:
                    print(f"\n🎉 Finished {name}. All channels complete!")
                    
            write_speaker_detection_log(all_stats, operation_name="All Channels Full Scrape")
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