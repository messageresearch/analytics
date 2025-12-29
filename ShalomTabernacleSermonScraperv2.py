import requests
from bs4 import BeautifulSoup
import concurrent.futures
import re
import yt_dlp
from datetime import datetime

# --- CONFIGURATION ---
BASE_URL = "https://www.shalomtabernacle.com/sermons/"
TOTAL_PAGES = 37
WORKER_THREADS = 6

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# --- YOUTUBE LOGIC ---
def get_youtube_metadata(video_url):
    if not video_url or "youtube" not in video_url:
        return "N/A", "N/A"

    ydl_opts = {
        'quiet': True,
        'skip_download': True,
        'no_warnings': True,
        'extractor_args': {'youtube': {'player_client': ['android', 'web']}}, 
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
            title = info.get('title', 'N/A')
            description = info.get('description', '')
            upload_date = info.get('upload_date') # YYYYMMDD
            
            # Intelligent Date Parsing from Description
            real_date = None
            if description:
                match = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2}(?:st|nd|rd|th)?(?:,)?\s+\d{4}', description, re.IGNORECASE)
                if match:
                    try:
                        date_str = match.group(0)
                        clean_date = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str)
                        for fmt in ["%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y"]:
                            try:
                                # Return YYYY-MM-DD for consistency with main script
                                real_date = datetime.strptime(clean_date, fmt).strftime("%Y-%m-%d")
                                break
                            except ValueError:
                                continue
                    except:
                        pass
            
            if not real_date and upload_date:
                # Convert YYYYMMDD to YYYY-MM-DD
                try:
                    real_date = datetime.strptime(upload_date, "%Y%m%d").strftime("%Y-%m-%d")
                except:
                    real_date = upload_date

            return title, (real_date if real_date else "0000-00-00")

    except Exception:
        return "Error Fetching YouTube Data", "0000-00-00"

# --- WEBSITE LOGIC ---
def extract_speaker(soup):
    text_content = soup.get_text()
    match = re.search(r'(?:Preacher|Minister|Speaker)\s*:\s*([^\n\r|]+)', text_content, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    meta_tags = soup.find_all('div', class_=re.compile(r'meta|author|entry-content'))
    for tag in meta_tags:
        if "Israel Powe" in tag.text:
            return "Pastor Israel Powe"
    return "Unknown Speaker"

def extract_youtube_url_from_page(soup):
    # 1. iFrames
    for iframe in soup.find_all('iframe'):
        src = iframe.get('src', '')
        if 'youtube' in src or 'youtu.be' in src:
            vid_id = re.search(r'embed/([a-zA-Z0-9_-]+)', src)
            if vid_id:
                return f"https://www.youtube.com/watch?v={vid_id.group(1)}"
    # 2. Block Embeds
    for div in soup.find_all(class_='wp-block-embed-youtube'):
        iframe = div.find('iframe')
        if iframe:
            src = iframe.get('src', '')
            vid_id = re.search(r'embed/([a-zA-Z0-9_-]+)', src)
            if vid_id:
                return f"https://www.youtube.com/watch?v={vid_id.group(1)}"
    # 3. Direct Links
    for a in soup.find_all('a', href=True):
        href = a['href']
        if ('youtube.com/watch' in href or 'youtu.be/' in href) and 'channel' not in href:
            return href
    return None

def process_single_sermon(page_url):
    try:
        response = requests.get(page_url, headers=HEADERS, timeout=15)
        if response.status_code != 200: return None
        soup = BeautifulSoup(response.text, 'html.parser')
        
        speaker = extract_speaker(soup)
        youtube_url = extract_youtube_url_from_page(soup)
        
        if not youtube_url: return None

        yt_title, yt_date = get_youtube_metadata(youtube_url)
        
        if yt_title == "N/A":
            page_title = soup.find('h1')
            if page_title: yt_title = page_title.get_text(strip=True)

        return {
            'Sermon Date': yt_date,
            'Sermon Title': yt_title,
            'Speaker': speaker,
            'YouTube URL': youtube_url
        }
    except Exception:
        return None

def fetch_sermons(max_pages=3):
    """
    Returns a list of video objects formatted specifically for update_sermons52.py
    """
    # If limit is passed as None (from main script args.limit), set to reasonable default
    # If limit is 0 or very high, use TOTAL_PAGES
    if max_pages is None:
        scan_limit = 3 # Default to just checking new stuff if no limit specified
    else:
        scan_limit = min(max_pages, TOTAL_PAGES)
        
    print(f"   🌐 [Website Scraper] Scanning first {scan_limit} pages of Shalom Tabernacle...")

    # 1. Harvest Links
    sermon_urls = []
    for page in range(1, scan_limit + 1):
        url = BASE_URL if page == 1 else f"{BASE_URL}page/{page}/"
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            soup = BeautifulSoup(r.text, 'html.parser')
            
            articles = soup.find_all(['article', 'div'], class_=re.compile(r'post|entry|type-post'))
            found_count = 0
            for art in articles:
                h_tag = art.find(['h1', 'h2', 'h3'])
                if h_tag and h_tag.find('a'):
                    sermon_urls.append(h_tag.find('a')['href'])
                    found_count += 1
                else:
                    for a in art.find_all('a', href=True):
                        href = a['href']
                        if '/sermons/' in href and '/category/' not in href and '/tag/' not in href and href != BASE_URL:
                            sermon_urls.append(href)
                            found_count += 1
                            break
        except Exception:
            continue
            
    unique_links = list(set(sermon_urls))
    print(f"   🌐 [Website Scraper] Found {len(unique_links)} sermon pages. Extracting data...")

    # 2. Process
    formatted_videos = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
        future_to_url = {executor.submit(process_single_sermon, url): url for url in unique_links}
        for future in concurrent.futures.as_completed(future_to_url):
            data = future.result()
            if data and "youtube" in data.get('YouTube URL', ''):
                vid_url = data['YouTube URL']
                try:
                    video_id = vid_url.split('v=')[-1].split('&')[0] if 'v=' in vid_url else vid_url.split('/')[-1]
                except:
                    video_id = "unknown"

                # mimic scrapetube structure + add manual overrides
                formatted_videos.append({
                    'videoId': video_id,
                    'title': {'runs': [{'text': data['Sermon Title']}]},
                    'manual_date': data['Sermon Date'],
                    'manual_speaker': data['Speaker']
                })
                
    return formatted_videos

if __name__ == "__main__":
    vids = fetch_sermons(max_pages=1)
    print(f"Found {len(vids)} videos.")