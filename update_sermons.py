
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
import unicodedata
import signal
import sys
import threading
import concurrent.futures
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
SPEAKERS_CONFIG_FILE = "config/speakers_config.json"
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
    "live", "stream", "update", "announcement", "q&a", "unknown", "speaker",
    "discussion", "teaching", "preaching", "song", "music", "choir", "harmony",
    "guest", "minister", "ministry", "revival", "conference",
    "report", "mission", "missions", "position", "clip", "wedding", "book", "items",
    "carriers", "vessel", "partnership", "seed", "garden", "situations",
    "control", "life", "power", "glory", "faith", "love", "hope", "fear",
    "video", "series", "restoration", "process", "year", "month",
    "day", "hour", "answer", "me", "you", "us", "them", "it", "words",
    "program", "skit", "singing", "drama", "play", "memorial", "celebration",
    "vbs", "cancel", "culture", "night", "altar", "call", "civil", "war", "project",
    # song lyrics / sermon title words
    "here", "room", "enemies", "scattered", "masterpiece", "holding", "another", "fire",
    "grudges", "holy", "convocations", "great", "thou", "deer", "because",
    "choose", "die", "need", "thee", "every", "speak", "stand", "courage", "reign",
    "amazed", "thank", "want", "more", "worthy", "vida", "near", "lost",
    "friend", "alpha", "parallel", "carried", "table",
    "crossed", "over", "token", "applied", "display", "real", "id", "face",
    "ready", "translation", "cycle", "death", "approval", "works", "kingdom", "sessions",
    "end", "time", "expectations", "method", "vindicated", "slaves", "no", "longer",
    "vs", "versus", "obediencia", "desobediencia", "light", "darkness", "manifest", "manifestation",
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
    "miercoles", "ibikorwa", "ikirundi", "vy", "vas",
    # Spanish radio program terms (Radio La Voz Del Tiempo Final)
    "oracion", "mundial", "devocional", "matutino", "despertar", "diario",
    # Audit found terms
    "condescendiendo", "milagro", "morar", "present", "tense", "messiah", "gastronomics",
    "intelligence", "resonance", "conception", "resurrections", "cooking", "cantantando",
    "adoracion", "watching", "fulfilled", "pleasing", "ngisebenzise", "moya",
    "oyingcwele", "boldness", "access", "confidence", "badger", "skin", "mastering",
    "circumstances", "separation", "brings", "inspiration", "pentecost", "jubilee",
    "million", "miracles", "anointing", "burial", "strengthen", "feeble", "knees",
    "excuses", "approaching", "perfection", "striving", "corruption", "choice",
    "revealing", "progressive", "sentence", "harlot", "knoweth",
    "salvation", "without", "iglesia", "tomalo", "vuelta", "diffrent", "different", "rain", 
    "missions",
    # Audit 2026-01-08 additions
    "overcomer", "resting", "authority", "capitalize", "victory", "hearken", "diligently", 
    "unto", "scared", "battle", "children", "spoken", "word", "identified", "treasure", 
    "transitions", "stepping", "standing", "serving", "rising", "rejoicing", "reflecting",
    "preserving", "preparing", "positive", "positioned", "peace", "overcoming", "mothers",
    "members", "living", "grace", "foot", "follow", "father", "drink", "desperation",
    "creatures", "created", "christmas", "beyond", "possessing", "restored", "thanksgiving",
    "teach", "seeds", "keep", "identity", "hold", "delight", "count", "choices", "build",
    "unclogging", "trophies", "seeing", "recognizing", "prevailing", "pressing", "ordained",
    "liberty", "elected", "creating", "suffer", "little",
    # Note: "long" removed - it's a valid surname (Ronnie Long, etc.)
    # Spanish/English Audit 2026-01-08 Round 2
    "sucesor", "circuncision", "poder", "vivificador", "viendo", "invisible", "tengo", 
    "bases", "regalo", "perfecto", "acercarnos", "importa", "dia", "adopcion", "buscando", 
    "bendicion", "aprovechando", "momento", "importancia", "noviasgo", "puedo", "crecer", 
    "quiere", "usted", "salvacion", "mundo", "necesita", "ayuda", "nuevas", "inexhaustible",
    "crucial", "moments", "edificando", "refocus", "moviendonos", "maestro", "stewardship",
    "calzados", "coming", "deja", "exito", "formando", "genesis", "gensis", "grupos", 
    "jose", "napa", "orando", "tema", "todo", "ya", "arrepentimiento", "condiciones", 
    "provando", "un", "dvfellowship", "interviews", "interview",
    "novia", "esposa", "cree", "creer", "solamente", "religion", "weakness", "strength",
    "proposito", "despierta", "reconociendo", "quienes", "somos", "frecuencia", "fundamento",
    "fundamental", "principio", "profundizarse", "resurrection", "resurreccion", "esperando",
    "report", "mission", "missions", "rain", "different", "diffrent",
    # Audit 2026-01-08 Round 3
    "herederos", "religion", "solamente", "interviews", "presencia", "santisimo", "moments", 
    "chapters", "experience", "election", "forgiveness", "praise", "authority", "interview",
    "interviewing", "interviewed", "interviewee", "interviewer", "conviction", "perseverancia",
    "crisis", "tabernaculo", "obra", "maestra", "spiritual", "supernatural", "throw", "jonah", "jonas",
    "genetics", "overboard",
    # Deep Audit additions
    "believers", "sisters", "brothers", "spirits", "musical", "banquet", "statements", 
    "codes", "restoring", "births", "vanities", "alone", "faithful", "deliver", 
    "voices", "healing", "vision", "walking", "shoes", "ashamed", "remaineth", 
    "submission", "lived", "rights", "closing", "belittling", "wounded", "lying",
    "peculiar", "rapture", "deliverer", "unashamed", "provision", "conscious",
    # Round 2 Deep Audit Additions
    "understanding", "pains", "reports", "modern", "always", "story", "concerned",
    "suddenly", "special", "specials", "tethered", "gathered", "showdown", "overloaded",
    "unveiling", "libertad", "crucificaron", "heaven", "comments", "change", "response",
    "dimensions", "beauty", "forth", "bones", "rebels", "givers",
    "higher", "hurry", "form", "bajado", "doing", "done", "sastre", "lado",
    "loveth", "remains", "animal", "temor", "quickening", "spirit",
    # Round 3 Deep Audit Additions (Safe)
    "shaddai", "perezoso", "leakage", "weaponry", "sparrows", "possession", "virgins", "foundation", "shepherd",
    "cravings", "manifested", "media", "servant", "eagle", "revelacion", "hid", "teofania", "tastes", "crabgrass",
    "symmetry", "meat", "seals", "yerushaliyim", "persistiendo", "mentality", "vessels", "secreto", "yokes",
    "affidavit", "reconciliation", "unlimited", "resources", "dolorosa", "recognition", "arcangel", "convention",
    "framing", "reality", "whiter", "forever", "awaits", "indeed", "nursing", "weds", "visit", "visiting",
    "homegoing", "smell", "scent", "odor", "perverted", "pure", "language", "nuggets", "deception", "reflected",
    "calling", "ground", "walls", "savage", "broken", "calling",
    # Round 4 Deep Audit Additions
    "gauging", "success", "edification", "sangre", "attitude", "tabernacled", "conclusion", "vertical", "receiving", 
    "assembly", "required", "choosing", "prophecy", "apostasy", "pleasure", "question", "holiness", "humility", 
    "blessings", "forgetting", "workers", "along", "front", "mas", "mental", "deity", "que", "heavenly", "set",
    "places", "apart", "redeeming", "believing", "marriage",
    # Round 5 Deep Audit Additions (Safe)
    "seminar", "hearing", "finding", "thoughts", "friends", "jealousy", "knowing", "beholding", "running", 
    "commissioned", "missionary", "welcome", "bienvenidos", "grit", "stability", "nature", "family", 
    "group", "sunday",
    # Round 6 Deep Audit Additions
    "comings", "future", "events", "giving", "losing", "enduring", "thread", "scarlet", "rahab", "reporte", 
    "misionero", "medley", "loving", "streaming", "error", "wisdom", "violent", "christianity", "throne", 
    "dying", "confronting", "youth", "issues", "birthday", "contending", "whom",
    # Round 7 Deep Audit Additions (Junk Phrases)
    "and when", "and take", "and take the helmet", "and then", "there the eagles will gather",
    "forsaken then crowned excerpt", "le témoignage dun vrai temoin", "la parábola de la levadura",
    "miembros vivientes.. expectativa", "el misterio de su voluntad", "el templo de la dinámica",
    "fue asi familia espinoza sequera", "restaurando la gloria", "total dependency and surrender",
    "and when the tempter comes", "true sons, born sons, filled sons", "blow a trumpet, sound an alarm",
    "boundaries starve unbelief and feed", "extrait adoration, louange",
    "ive been changed the cockman", "young men and women renewing",
    "hannah slachta, too many times", "parental influenceinfluencia de padres",
    "toda la armadura de dios", "at-one-ment", "and then, dead men",
    "los hijos de dios manifestados", "hurt people, hurt people iii",
    "hurt people, hurt people ii", "hurt people, hurt people",
    "humiliation, then glorification", "la guerra espiritual",
    "los negocios de mi padre", "el camino provisto por dios", "shamgar a man convinced",
    "rompiendo el ciclo", "el absoluto the absolute", "la gran galeria de dios",
    "una perla de gran valor", "profundizandonos con dios going deeper",
    "aun con tentaciones", "la segunda venida", "declarando victoria",
    "la palabra sangrante", "el hombre espiritual", "fe disruptiva",
    "promesas rotas", "hay esperanza", "bendiceme", "enfermedades espirituales",
    "desempaquear para irnos", "peregrinos extranjeros", "quien soy",
    "locos lindosbeautiful crazy", "la piedra rechazada", "la puerta abierta",
    "recorriendo la biblia", "despues de la batalla", "graduation ceremony",
    "awards ceremony", "miracle baby", "sis carla price",
    # Round 8 Deep Audit Additions (New Junk)
    "abierto", "battlefield", "capitulo", "changed", "character", "deliverance", 
    "donde esta tu fe", "elohim", "faithfulness", "hallelujah", "idolatry", 
    "jericho", "kingdoms", "maturity", "overcomers", "preocupado", "preparation", 
    "sardis", "selahammahlekoth", "shalom", "testify", "thirst", "solomon", "esthers",
    "familiarity breeds contempt", "the return", "de los echos", "de los hechos",
    # Round 9 Deep Audit - Single Word False Positives (titles misdetected as speakers)
    "perfect", "esther", "mamelodi", "headstone", "fourth", "divine", "complete",
    "inheritance", "demonology", "congregational", "admiration", "adoption", "announcement",
    "assurance", "bitterness", "commitment", "communion", "conducted", "connection",
    "conviction", "dedication", "deliverance", "demonstration", "dependence", "determination",
    "devotion", "direction", "discernment", "dominion", "elevation", "encouragement",
    "endowment", "engagement", "enlightenment", "establishment", "examination", "expectation",
    "expression", "extension", "extraction", "formation", "foundation", "generation",
    "glorification", "graduation", "habitation", "identification", "illumination", "imitation",
    "impartation", "implementation", "implication", "incarnation", "inclination", "indication",
    "infiltration", "information", "inhabitation", "initiation", "innovation", "inspiration",
    "installation", "instruction", "integration", "intensification", "intercession", "interpretation",
    "intervention", "introduction", "investigation", "invitation", "irrigation", "isolation",
    "jubilation", "justification", "lamentation", "liberation", "limitation", "location",
    "manifestation", "meditation", "memorization", "ministration", "modification", "motivation",
    "multiplication", "navigation", "negotiation", "observation", "occupation", "operation",
    "opposition", "ordination", "orientation", "origination", "participation", "penetration",
    "perception", "perfection", "persecution", "petition", "population", "position",
    "possession", "predestination", "prediction", "preparation", "presentation", "preservation",
    "proclamation", "progression", "prohibition", "projection", "promotion", "pronunciation",
    "proportion", "proposition", "protection", "provocation", "publication", "qualification",
    "quotation", "realization", "recitation", "recognition", "recommendation", "reconciliation",
    "recreation", "redemption", "reflection", "reformation", "regeneration", "regulation",
    "reincarnation", "rejection", "relation", "relaxation", "relocation", "renovation",
    "repetition", "representation", "reproduction", "reputation", "reservation", "resignation",
    "resolution", "restoration", "restriction", "resurrection", "retaliation", "revelation",
    "revolution", "sanctification", "satisfaction", "separation", "simplification", "situation",
    "specification", "speculation", "stabilization", "stimulation", "stipulation", "submission",
    "substitution", "supplication", "supposition", "suspension", "temptation", "termination",
    "transformation", "translation", "transportation", "tribulation", "unification", "validation",
    "verification", "vindication", "visitation", "vocalization",
    # Round 9 - Time/Service patterns as single words
    "monday", "tuesday", "thursday", "friday", "saturday",
    # Round 9 - Common title words that appear as single-word speakers
    "token", "seals", "breach", "stature", "shuck", "heir", "anointed", "attraction",
    "paradox", "parallel", "masterpiece", "unwrapped", "uncentered", "unglazed", "unwaxed",
    "sonlit", "brilliant", "resolving", "deformation", "conducted", "composed", "performed",
    "magnum", "opus", "resonance", "graceshame", "lifexdeath", "musics", "movement",
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
    "New Years Day Items", "Picture Slideshow", "Church Service",
    # Time-based patterns that get detected as speakers
    "Sunday Morning", "Sunday Evening", "Sunday Night", "Sunday Service",
    "Monday Morning", "Monday Evening", "Monday Night", "Monday Service",
    "Tuesday Morning", "Tuesday Evening", "Tuesday Night", "Tuesday Service",
    "Wednesday Morning", "Wednesday Evening", "Wednesday Night", "Wednesday Service",
    "Thursday Morning", "Thursday Evening", "Thursday Night", "Thursday Service",
    "Friday Morning", "Friday Evening", "Friday Night", "Friday Service",
    "Saturday Morning", "Saturday Evening", "Saturday Night", "Saturday Service",
    "Sunday Morning Service", "Wednesday Evening Service", "Friday Evening Service",
    "Communion Service", "Youth Service", "Baptismal Service", "Deliverance Service",
    "Prayer Service", "Worship Service", "Memorial Service", "Dedication Service",
    # Single words that are categories, not speakers
    "Congregation", "Congregational", "Choir", "Childrens",
}

# --- GRACEFUL SHUTDOWN STATE ---
_shutdown_requested = threading.Event()

def handle_shutdown_signal(signum, frame):
    """Handle SIGINT (Ctrl+C) and SIGTERM gracefully."""
    if _shutdown_requested.is_set():
        print("\n\n⚠️  Force shutdown requested. Exiting immediately...")
        sys.exit(1)

    signal_name = "SIGINT (Ctrl+C)" if signum == signal.SIGINT else "SIGTERM"
    print(f"\n\n🛑 {signal_name} received. Finishing current church before stopping...")
    print("   (Press Ctrl+C again to force quit)")
    _shutdown_requested.set()

def should_shutdown():
    """Check if graceful shutdown was requested."""
    return _shutdown_requested.is_set()

def reset_shutdown_state():
    """Reset shutdown state for fresh runs."""
    _shutdown_requested.clear()

# --- YOUTUBE TIMEOUT WRAPPER ---
YOUTUBE_TIMEOUT_SECONDS = 30

def youtube_with_timeout(url, client=None, use_oauth=False, allow_oauth_cache=True, timeout=YOUTUBE_TIMEOUT_SECONDS):
    """
    Create a YouTube object with a timeout to prevent hanging on problematic videos.

    Args:
        url: YouTube video URL
        client: Client type (e.g., 'WEB', 'ANDROID') - if None, uses default
        use_oauth: Whether to use OAuth
        allow_oauth_cache: Whether to allow OAuth caching
        timeout: Maximum seconds to wait (default: 30)

    Returns:
        YouTube object if successful, None if timeout or error
    """
    def create_youtube():
        if client:
            yt = YouTube(url, client=client, use_oauth=use_oauth, allow_oauth_cache=allow_oauth_cache)
        else:
            yt = YouTube(url, use_oauth=use_oauth, allow_oauth_cache=allow_oauth_cache)
        # Force metadata fetch to happen now (this is what can hang)
        _ = yt.title
        return yt

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(create_youtube)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            print(f"      ⏱️ TIMEOUT ({timeout}s) fetching: {url[:60]}...")
            return None
        except Exception as e:
            # Re-raise the exception so caller can handle it
            raise e

# --- ATOMIC FILE OPERATIONS ---
def atomic_write_csv(filepath, rows, fieldnames):
    """Write CSV atomically using temp file + rename to prevent corruption."""
    temp_path = filepath + '.tmp'
    try:
        with open(temp_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(temp_path, filepath)  # Atomic on POSIX
    except Exception as e:
        # Clean up temp file on error
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
                pass
        raise e

def atomic_write_json(filepath, data, sort_if_list=True):
    """Write JSON atomically using temp file + rename to prevent corruption."""
    temp_path = filepath + '.tmp'
    try:
        with open(temp_path, 'w', encoding='utf-8') as f:
            if sort_if_list and isinstance(data, (list, set)):
                json.dump(sorted(list(data)), f, indent=2, ensure_ascii=False)
            else:
                json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(temp_path, filepath)  # Atomic on POSIX
    except Exception as e:
        # Clean up temp file on error
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
                pass
        raise e

# --- MASTER CSV GENERATION ---
def generate_master_csv():
    """
    Combine all church summary CSV files into a single master CSV file.
    Writes to: data/master_sermons_summary.csv
    """
    import glob
    
    master_file_path = os.path.join(DATA_DIR, "master_sermons_summary.csv")
    summary_files = glob.glob(os.path.join(DATA_DIR, "*_Summary.csv"))
    
    if not summary_files:
        print("⚠️ No summary CSV files found to combine.")
        return
    
    # Read all CSV files and combine
    all_rows = []
    fieldnames = None
    
    for csv_file in sorted(summary_files):
        try:
            with open(csv_file, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f)
                if fieldnames is None:
                    fieldnames = reader.fieldnames
                for row in reader:
                    all_rows.append(row)
        except Exception as e:
            print(f"⚠️ Error reading {csv_file}: {e}")
    
    if not all_rows or not fieldnames:
        print("⚠️ No data to write to master CSV.")
        return
    
    # Write master CSV
    try:
        with open(master_file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"✅ Master CSV generated: {master_file_path} ({len(all_rows):,} sermons)")
    except Exception as e:
        print(f"❌ Error writing master CSV: {e}")

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
    logs_dir = "logs/speaker_detection"
    os.makedirs(logs_dir, exist_ok=True)
    filename = f"speaker_detection_log_{timestamp.strftime('%Y%m%d_%H%M%S')}.txt"
    filepath = os.path.join(logs_dir, filename)
    
    total = stats_dict.get('total_processed', 0)
    detected = stats_dict.get('speakers_detected', 0)
    unknown = stats_dict.get('unknown_speakers', 0)
    unknown_before = stats_dict.get('unknown_speakers_before', None)
    
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
    ]
    if unknown_before is not None:
         lines.append(f"Unknown Speakers (Before):  {unknown_before:,}")
         lines.append(f"Unknown Speakers (After):   {unknown:,}")
    else:
         lines.append(f"Unknown Speakers:           {unknown:,}")

    lines.extend([
        f"Speakers Detected:          {detected:,}",
        f"Detection Rate:             {detection_rate:.1f}%",
        "",
    ])
    
    # Add optional stats if present
    if 'speakers_corrected' in stats_dict:
        lines.append(f"Speakers Corrected:         {stats_dict['speakers_corrected']:,}")
    if 'speakers_redetected' in stats_dict:
        lines.append(f"Speakers Re-detected:       {stats_dict['speakers_redetected']:,}")
    if 'speakers_changed_to_unknown' in stats_dict:
        lines.append(f"Changed To Unknown Speaker: {stats_dict['speakers_changed_to_unknown']:,}")
    if 'transcripts_updated' in stats_dict:
        lines.append(f"Transcripts Updated:        {stats_dict['transcripts_updated']:,}")
    if 'summaries_updated' in stats_dict:
        lines.append(f"Summary Entries Updated:    {stats_dict['summaries_updated']:,}")
    if 'skipped_same' in stats_dict:
        lines.append(f"Skipped (Same Speaker):     {stats_dict['skipped_same']:,}")
    if 'skipped_not_found' in stats_dict:
        lines.append(f"Skipped (File Not Found):   {stats_dict['skipped_not_found']:,}")

    # Speaker list inventory deltas (speakers.json)
    if any(k in stats_dict for k in ('unique_speakers_before', 'unique_speakers_after', 'speakers_added', 'speakers_removed')):
        lines.extend([
            "",
            "SPEAKER LIST INVENTORY (speakers.json)",
            "-" * 40,
        ])
        if 'unique_speakers_before' in stats_dict:
            lines.append(f"Unique Speakers Before:     {stats_dict['unique_speakers_before']:,}")
        if 'unique_speakers_after' in stats_dict:
            lines.append(f"Unique Speakers After:      {stats_dict['unique_speakers_after']:,}")
        if 'speakers_added' in stats_dict:
            lines.append(f"New Speakers Added:         {stats_dict['speakers_added']:,}")
        if 'speakers_removed' in stats_dict:
            lines.append(f"Speakers Removed:           {stats_dict['speakers_removed']:,}")

    # CSV files processed
    if stats_dict.get('csv_files_processed'):
        csv_files = list(dict.fromkeys(stats_dict['csv_files_processed']))  # stable de-dupe
        lines.extend([
            "",
            "SUMMARY CSV FILES PROCESSED",
            "-" * 40,
            f"Count: {len(csv_files)}",
        ])
        max_list = 200
        for p in csv_files[:max_list]:
            lines.append(f"  • {p}")
        if len(csv_files) > max_list:
            lines.append(f"  • ... ({len(csv_files) - max_list} more omitted)")
    
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


def write_heal_speaker_corrections_logs(corrections, data_dir, operation_name="Heal Archive"):
    """Write speaker correction logs (detailed + FROM/TO summary) to CSV files."""
    if not corrections:
        return None, None

    timestamp = datetime.datetime.now()
    logs_dir = "logs/healing"
    os.makedirs(logs_dir, exist_ok=True)

    safe_op = re.sub(r'[^A-Za-z0-9_-]+', '_', operation_name).strip('_')
    stamp = timestamp.strftime('%Y%m%d_%H%M%S')

    detailed_path = os.path.join(logs_dir, f"heal_speaker_corrections_{safe_op}_{stamp}.csv")
    summary_path = os.path.join(logs_dir, f"heal_speaker_from_to_{safe_op}_{stamp}.csv")

    fieldnames = [
        "church",
        "date",
        "title",
        "url",
        "from_speaker",
        "to_speaker",
        "reason",
    ]

    try:
        with open(detailed_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in corrections:
                writer.writerow({k: row.get(k, "") for k in fieldnames})
    except Exception as e:
        print(f"⚠️ Failed to write heal corrections log: {e}")
        detailed_path = None

    from_to_counts = {}
    for row in corrections:
        key = (row.get('from_speaker', ''), row.get('to_speaker', ''))
        from_to_counts[key] = from_to_counts.get(key, 0) + 1

    try:
        with open(summary_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["from_speaker", "to_speaker", "count"])
            writer.writeheader()
            for (from_s, to_s), count in sorted(from_to_counts.items(), key=lambda x: (-x[1], x[0][0], x[0][1])):
                writer.writerow({"from_speaker": from_s, "to_speaker": to_s, "count": count})
    except Exception as e:
        print(f"⚠️ Failed to write heal FROM/TO summary: {e}")
        summary_path = None

    return detailed_path, summary_path


def _casefold_speaker(name: str) -> str:
    return speaker_casefold_key(name)


def compute_speaker_inventory_delta(before_set, after_set):
    """Compute case-insensitive speaker inventory delta for speakers.json."""
    before_keys = {_casefold_speaker(s) for s in (before_set or set()) if (s or '').strip()}
    after_keys = {_casefold_speaker(s) for s in (after_set or set()) if (s or '').strip()}

    before_keys.discard(_casefold_speaker("Unknown Speaker"))
    after_keys.discard(_casefold_speaker("Unknown Speaker"))

    return {
        'unique_speakers_before': len(before_keys),
        'unique_speakers_after': len(after_keys),
        'speakers_added': len(after_keys - before_keys),
        'speakers_removed': len(before_keys - after_keys),
    }

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
    r'Reverend', r'Elder', r'Deacon', r'Minister', r'Min\.?', r'Dr\.?', r'Bishop',
    r'Apostle', r'Evangelist', r'Prophet', r'Hno\.?', r'Hermano', r'Hermana',
    r'Founder', r'Associate\s+Pastor', r'Ministering', r'Ministered\s+by',
    r'Br\.?', r'Sr\.?', r'Ptr\.?', r'Pst\.?', r'Past\.?', r'Founding\s+Pastor',
]
HONORIFIC_PATTERN = r'(?:' + '|'.join(HONORIFICS) + r')'

# Common words that indicate a speaker follows
SPEAKER_INDICATORS = [
    r'by', r'with', r'from', r'featuring', r'feat\.?', r'ft\.?', 
    r'ministered\s+by', r'preached\s+by', r'delivered\s+by',
    r'speaker[:\s]', r'minister[:\s]', r'interviews',
]

# Pattern for name-like sequences (capitalized words)
# EXCLUDE common title starters/connectors from continuing a name chain
NAME_STOP_WORDS = r"(?:No|The|A|An|Is|In|On|At|To|For|Of|By|With|My|His|Her|Your|Our|Who|What|Where|When|Why|How|\d|Wednesday|Sunday|Monday|Tuesday|Thursday|Friday|Saturday|Morning|Evening|Night|Service|Meeting|Sermon|Part|Pt|Series)"
# Updated Name Pattern:
# 1. Start with Capital word (NOT a stop word) - OR two capital letters (e.g. IT) - OR specific lowercase name "martin"
# 2. Continue with Capital words (NOT stop words) preceded by separator
# 3. Supports accented characters (Latin-1 Supplement \xC0-\xFF) for names like "Magaña", "René"
NAME_PATTERN = r"(?:(?!" + NAME_STOP_WORDS + r"\b)(?:[A-Z\xC0-\xD6\xD8-\xDE][a-z\xDF-\xF6\xF8-\xFF]+|[A-Z]{2,}|martin)(?:[\s'-](?!" + NAME_STOP_WORDS + r"\b)(?:[A-Z\xC0-\xD6\xD8-\xDE][a-z\xDF-\xF6\xF8-\xFF]+|[A-Z]{2,}))*)"

# Words/patterns that should NOT be considered names (expanded from speaker_detector.py)
NON_NAME_PATTERNS = [
    r'^Part\b', r'^Pt\.?\b', r'^Episode\b', r'^Ep\.?\b', r'^Vol\.?\b',
    r'^The\b', r'^A\b', r'^An\b', r'^This\b', r'^That\b', r'^Our\b', r'^My\b',
    r'^God\b', r'^Jesus$', r'^Christ\b', r'^Lord\b', r'^Holy\b', r'^Spirit\b',
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
                data = json.load(f)

            # speakers.json: dedupe case-insensitively to avoid duplicates like
            # "John Smith" vs "john smith".
            if os.path.basename(filepath) == SPEAKERS_FILE:
                best_by_key = {}
                for raw in data:
                    s = normalize_nfc_text(" ".join((raw or '').split()).strip())
                    if not s:
                        continue
                    key = speaker_casefold_key(s)
                    current = best_by_key.get(key)
                    if current is None:
                        best_by_key[key] = s
                        continue

                    # Prefer non-ALLCAPS and non-all-lowercase variants when possible.
                    def rank(v: str):
                        return (1 if v.isupper() else 0, 1 if v.islower() else 0, v)

                    if rank(s) < rank(current):
                        best_by_key[key] = s

                return set(best_by_key.values())
            
            # If it is a dictionary (config file), return as is.
            if isinstance(data, dict):
                return data

            return set(data)
        except json.JSONDecodeError as e:
            # Protect against silently treating a hand-edited speakers.json as empty.
            if os.path.basename(filepath) == SPEAKERS_FILE:
                print(f"\n❌ ERROR: {SPEAKERS_FILE} is not valid JSON and could be overwritten if the script continues.")
                print(f"   Fix the JSON formatting (missing commas/quotes/brackets) and re-run.")
                print(f"   Details: {e}\n")
                raise
            return set()
        except Exception:
            return set()
    return set()


def normalize_nfc_text(value: str) -> str:
    try:
        return unicodedata.normalize('NFC', value or '')
    except Exception:
        return value or ''


def speaker_casefold_key(name: str) -> str:
    return normalize_nfc_text(" ".join((name or "").split())).casefold()


def build_known_speakers_casefold_map(known_speakers) -> dict:
    return {speaker_casefold_key(s): s for s in known_speakers if (s or "").strip()}

def save_json_file(filepath, data):
    try:
        if os.path.basename(filepath) == SPEAKERS_FILE:
            # Deduplicate case-insensitively on write.
            best_by_key = {}
            for raw in data:
                s = normalize_nfc_text(" ".join((raw or '').split()).strip())
                if not s:
                    continue
                key = speaker_casefold_key(s)
                current = best_by_key.get(key)
                if current is None:
                    best_by_key[key] = s
                    continue

                def rank(v: str):
                    return (1 if v.isupper() else 0, 1 if v.islower() else 0, v)

                if rank(s) < rank(current):
                    best_by_key[key] = s

            data = set(best_by_key.values())

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(sorted(list(data)), f, indent=2, ensure_ascii=False)
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


def video_matches_filter(video, filter_config, yt_object=None):
    """
    Check if a video matches the filter criteria.
    
    Args:
        video: Video dictionary with 'videoId' and optionally 'title', 'descriptionSnippet'
        filter_config: Dictionary with 'require_any' (list of strings) and 'match_in' (list: 'title', 'description')
        yt_object: Optional YouTube object for fetching full description
    
    Returns:
        True if the video matches any of the required terms, False otherwise
    """
    if not filter_config:
        return True  # No filter = include all
    
    require_any = filter_config.get('require_any', [])
    match_in = filter_config.get('match_in', ['title'])
    
    if not require_any:
        return True  # No required terms = include all
    
    # Get title
    try:
        title = video.get('title', {}).get('runs', [{}])[0].get('text', '') or video.get('title', '')
        if isinstance(video.get('title'), str):
            title = video.get('title')
    except:
        title = ''
    
    # Get description snippet from scrapetube data
    description = ''
    if 'description' in match_in:
        try:
            # Try to get description from various sources
            desc_snippet = video.get('descriptionSnippet', {})
            if isinstance(desc_snippet, dict):
                runs = desc_snippet.get('runs', [])
                description = ' '.join([r.get('text', '') for r in runs]) if runs else ''
            elif isinstance(desc_snippet, str):
                description = desc_snippet
            
            # If no description snippet and we have a YouTube object, fetch the full description
            if not description and yt_object:
                try:
                    description = yt_object.description or ''
                except:
                    pass
        except:
            pass
    
    # Check each required term
    for term in require_any:
        term_lower = term.lower()
        if 'title' in match_in and term_lower in title.lower():
            return True
        if 'description' in match_in and term_lower in description.lower():
            return True
    
    return False


def fetch_additional_channel_videos(channel_config, limit=None, days_back=None):
    """
    Fetch videos from an additional channel with filtering support.

    Args:
        channel_config: Dictionary with 'url', 'name', 'filter', and optionally 'date_format'
        limit: Maximum number of videos to fetch (None for all)
        days_back: Only include videos from the last N days (None for all)

    Returns:
        Tuple of (matching_videos, total_scanned, filtered_out)
    """
    channel_url = channel_config.get('url')
    channel_name = channel_config.get('name', 'Unknown')
    filter_config = channel_config.get('filter')
    
    if not channel_url:
        return [], 0, 0
    
    all_videos = []
    matching_videos = []
    
    try:
        # Get base channel URL
        base_url = channel_url.split('/streams')[0].split('/videos')[0].split('/featured')[0]
        
        # Fetch videos using scrapetube
        print(f"      🌐 Scanning channel: {channel_name}...")
        videos = list(scrapetube.get_channel(channel_url=base_url, content_type='videos', limit=limit))
        streams = list(scrapetube.get_channel(channel_url=base_url, content_type='streams', limit=limit))
        all_videos = videos + streams
        
        print(f"      📊 Found {len(all_videos)} videos total")
        
        # Apply date filter first if days_back is set
        if days_back:
            date_filtered = []
            for video in all_videos:
                published_time_text = video.get('publishedTimeText', {}).get('simpleText', '')
                if published_time_text and parse_published_time(published_time_text, max_days=days_back):
                    date_filtered.append(video)
            print(f"      📅 Date filter: {len(date_filtered)} videos within {days_back} days (skipped {len(all_videos) - len(date_filtered)} older)")
            all_videos = date_filtered

        if not filter_config:
            for video in all_videos:
                video['_from_additional_channel'] = True
                video['_additional_channel_name'] = channel_name
                video['_date_format'] = channel_config.get('date_format')
            return all_videos, len(all_videos), 0

        # Apply content filter
        require_terms = filter_config.get('require_any', [])
        print(f"      🔍 Filtering for: {', '.join(require_terms)}")

        for video in all_videos:
            if video_matches_filter(video, filter_config):
                # Mark the video with the source channel info
                video['_from_additional_channel'] = True
                video['_additional_channel_name'] = channel_name
                video['_date_format'] = channel_config.get('date_format')
                matching_videos.append(video)

        filtered_out = len(all_videos) - len(matching_videos)
        print(f"      ✅ {len(matching_videos)} videos matched filter ({filtered_out} filtered out)")
        
        return matching_videos, len(all_videos), filtered_out
        
    except Exception as e:
        print(f"      ⚠️ Error fetching additional channel: {e}")
        return [], 0, 0


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

    # Common metadata leakage into speaker field (e.g., "<Name> Date/Title/Venue")
    tokens = re.split(r'\s+', speaker.strip())
    if len(tokens) >= 3 and tokens[-1].lower() in {"date", "title", "venue"}:
        base2 = " ".join(tokens[:2]).strip()
        if is_valid_person_name(base2):
            return base2

    # Explicit non-speaker title leakage (known bad values observed in data)
    if s_lower in {
        "conferencia con dios",
        "confronting eternal",
        "correctly overcoming",
        "corriendo con paciencia",
        "cosmetic christians",
        "covenanted blood purging",
        "creating peace",
        "crossing jordan",
        "crying grace",
        "dark places",
        "empty vessels",
        "encourage yourself",
        "end time evangelism",
        "end-time transfromation",
        "enfocando nuestros pensamientos",
        "enlarging territories",
        "elohim operating",
        "elt adult",
        "embracing deity",
        "entertaining strangers",
        "eternal plan",
        "eternal purpose",
        "eternal redemption",
        "eternal separation",
        "etm tabernacles crossover",
        "everlasting consequences",
        "everlasting covenant",
        "exceeding righteousness",
        "except ye abide",
        "exceptional praise",
        "experiencing fulfillment",
        "experiencing gods",
        "extra chair",
        "face-to-face relationship",
        "faint yet pursuing",
        "faiths reaction",
        "false anointed ones",
        "false humility",
        "familiar places",
        "familiarity breeds contempt",
        "family idols",
        "family matters overview",
        "family positions",
        "family working together",
        "forsaken then crowned",
        "abstract title deed",
        "ill fly away",
        "i'll fly away",
        "infant seeds",
        "invasion insanity deliverance",
        "invert always invert",
        "investigating angels",
        "humble thyself",
        "humble yourself",
        "israel commemoration",
        "israel gods redemption",
        "ive been changed",
        "i've been changed",
        "ive tried",
        "i've tried",
        "only believe",
        "open channels",
        "open door",
        "original inspiration",
        "original sin",
        "josephs silver cup",
        "joshua parallels ephesians",
        "judgement seat",
        "lump without leaven",
        "phoenix lighthouse",
        "picture perfect",
        "piezas de rompecabezas",
        "plan possess plant",
        "precious promises",
        "prepared ground",
        "present context",
        "present darkness",
        "present tense manifestation",
        "present tense revelation",
        "promise island",
        "prophecy politics",
        "prosper lamour parfait",
        "pure heart",
        "rapture landscape",
        "rapturing conditin",
        "rapturing strength",
        "rapturing without",
        "rejected stone",
        "religion versus relationship",
        "remaining fruitful",
        "remaining vigilant",
        "remembering calvary",
        "remembering gods gift",
        "rescue story brother",
        "resolving evil",
        "respecting gods",
        "revelacion espiritual",
        "revelation brings strength",
        "revelation resolves complication",
        "rich young ruler",
        "right hand",
        "right now",
        "righteous mans reward",
    }:
        return "Unknown Speaker"

    # Targeted cleanup: remove the trailing word "Special" when it is clearly a suffix
    # (e.g., "Courtney Dexter Special" -> "Courtney Dexter").
    if re.match(r'^Courtney\s+Dexter\s+Special$', speaker, re.IGNORECASE):
        return "Courtney Dexter"

    # Targeted cleanup: remove trailing title leakage for a known speaker.
    # (e.g., "Aaron McGeary Bittersweet" -> "Aaron McGeary")
    if re.match(r'^Aaron\s+McGeary\s+Bittersweet$', speaker, re.IGNORECASE):
        return "Aaron McGeary"

    # Paul Haylett contamination fix
    # Examples (bad): "Paul Haylett Accepted", "Paul Haylett Anchored", ...
    # Canonical (good): "Paul Haylett"
    if re.match(r'^Paul\s+Haylett\b', speaker, re.IGNORECASE):
        tokens = re.split(r'\s+', speaker.strip())
        allowed_suffixes = {'jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv'}
        if len(tokens) == 2:
            return "Paul Haylett"
        if len(tokens) >= 3 and tokens[2].lower() in allowed_suffixes:
            return "Paul Haylett " + tokens[2].rstrip('.')
        return "Paul Haylett"

    # Targeted cleanup: strip leaked trailing words for known speakers
    if re.match(r'^Joseph\s+Coleman\s+April$', speaker, re.IGNORECASE):
        return "Joseph Coleman"
    if re.match(r'^Jose\s+Hernandez\s+Work$', speaker, re.IGNORECASE):
        return "Jose Hernandez"
    
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

    # --- ROUND 9 AUDIT ADDITIONS ---

    # Fix incomplete names
    if re.match(r'^H\s+Simmons$', speaker, re.IGNORECASE): return 'Henry Simmons'
    if re.match(r'^William\s+Marrion$', speaker, re.IGNORECASE): return 'Unknown Speaker'  # Incomplete Branham
    if re.match(r'^Apostle\s+Bernie$', speaker, re.IGNORECASE): return 'Unknown Speaker'  # Incomplete - need full name
    if re.match(r'^de\s+la$', speaker, re.IGNORECASE): return 'Unknown Speaker'  # Fragment

    # Block time-based patterns
    if re.match(r'^(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday)\s+(Morning|Evening|Night|Service)', speaker, re.IGNORECASE):
        return "Unknown Speaker"

    # Block single words that are clearly titles
    single_word_titles = {
        'perfect', 'esther', 'mamelodi', 'headstone', 'fourth', 'divine', 'complete',
        'inheritance', 'demonology', 'congregational', 'congregation', 'choir', 'childrens',
        'thomas', 'coleman', 'steven', 'bernie', 'william', 'daniel', 'abraham',
        'anthony', 'joseph', 'ronnie', 'brooks', 'joshua', 'emmanuel', 'carlos', 'faustin',
        'conducted', 'composed', 'performed', 'graceshame', 'lifexdeath', 'musics', 'movement',
        'token', 'seals', 'breach', 'stature', 'shuck', 'heir', 'anointed', 'attraction',
        'paradox', 'parallel', 'masterpiece', 'unwrapped', 'uncentered', 'unglazed', 'unwaxed',
        'sonlit', 'brilliant', 'resolving', 'deformation', 'magnum', 'opus', 'resonance',
    }
    if re.match(r'^[A-Z][a-z]+$', speaker):
        if speaker.lower() in single_word_titles:
            return "Unknown Speaker"

    # Block speakers starting with articles (sermon titles)
    if re.match(r'^(The|A|An)\s+[A-Z]', speaker):
        return "Unknown Speaker"

    # Block speakers containing Part/Pt numbers
    if re.search(r'\b(Pt\.?|Part)\s*\d', speaker, re.IGNORECASE):
        return "Unknown Speaker"

    # Block speakers with 4+ digit numbers (dates, codes)
    if re.search(r'\d{4,}', speaker):
        return "Unknown Speaker"

    # Block speakers with parentheses
    if '(' in speaker or ')' in speaker:
        return "Unknown Speaker"

    # Block speakers starting with numbers
    if re.match(r'^\d', speaker):
        return "Unknown Speaker"

    # Block gerund + preposition patterns
    if re.match(r'^[A-Z][a-z]+ing\s+(The|In|To|Of|With|On|A|An|For|At|By)\s+', speaker, re.IGNORECASE):
        return "Unknown Speaker"

    # --- END ROUND 9 AUDIT ADDITIONS ---

    # Chad Lamb contamination fix
    # Examples (bad): "Chad Lamb Access", "Chad Lamb Discernment", ...
    # Canonical (good): "Chad Lamb"
    if re.match(r'^Chad\s+Lamb\b', speaker, re.IGNORECASE):
        tokens = re.split(r'\s+', speaker.strip())
        # Keep legitimate suffixes like Jr/Sr/II/etc; otherwise collapse to the base name
        allowed_suffixes = {'jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv'}
        if len(tokens) == 2:
            return "Chad Lamb"
        if len(tokens) >= 3 and tokens[2].lower() in allowed_suffixes:
            return "Chad Lamb " + tokens[2].rstrip('.')
        return "Chad Lamb"

    # Diego Arroyo contamination fix
    # Examples (bad): "Diego Arroyo Gen", "Diego Arroyo Isa", "Diego Arroyo Ezekiel", ...
    # Canonical (good): "Diego Arroyo"
    if re.match(r'^Diego\s+Arroyo\b', speaker, re.IGNORECASE):
        tokens = re.split(r'\s+', speaker.strip())
        allowed_suffixes = {'jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv'}
        if len(tokens) == 2:
            return "Diego Arroyo"
        if len(tokens) >= 3 and tokens[2].lower() in allowed_suffixes:
            return "Diego Arroyo " + tokens[2].rstrip('.')
        return "Diego Arroyo"

    # Burley Williams contamination fix
    # Examples (bad): "Burley Williams Balm", "Burley Williams Living"
    # Canonical (good): "Burley Williams"
    if re.match(r'^Burley\s+Williams\b', speaker, re.IGNORECASE):
        tokens = re.split(r'\s+', speaker.strip())
        allowed_suffixes = {'jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv'}
        if len(tokens) == 2:
            return "Burley Williams"
        if len(tokens) >= 3 and tokens[2].lower() in allowed_suffixes:
            return "Burley Williams " + tokens[2].rstrip('.')
        return "Burley Williams"

    # Coleman contamination fix
    # Examples (bad): "Coleman August", "Coleman Puerto Rico", "Coleman Forest Hills"
    # Canonical (good): "Coleman"
    # Only collapses when the suffix looks like a month or a known location phrase.
    if re.match(r'^Coleman\b', speaker, re.IGNORECASE):
        s = speaker.strip()
        # Month suffix
        if re.match(r'^Coleman\s+(January|February|March|April|May|June|July|August|September|October|November|December)\b', s, re.IGNORECASE):
            return "Coleman"
        # Location/venue-like suffixes observed in data
        if re.match(r'^Coleman\s+Puerto\s+Rico\b', s, re.IGNORECASE):
            return "Coleman"
        if re.match(r'^Coleman\s+Forest\s+Hills\b', s, re.IGNORECASE):
            return "Coleman"

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

def is_likely_name_part(text):
    """Check if a comma-separated part looks like a name component."""
    clean = text.strip()
    if not clean: return False
    if not clean[0].isupper(): return False
    # Check against global invalid terms
    if clean.lower() in INVALID_NAME_TERMS: return False
    # Check commonly mistaken numeric/date parts
    if any(c.isdigit() for c in clean): return False
    return True

def clean_name(name):
    """Clean up extracted name - improved version."""
    if not name:
        return ""
    
    # 1. Handle "Name, Title" pattern where comma separates valid name from noise
    # ALSO handle "Name, Name" pattern (keep multiple names)
    if "," in name:
        parts = [p.strip() for p in name.split(",")]
        kept_parts = []
        if parts:
            kept_parts.append(parts[0]) # Always keep base
            
            for p in parts[1:]:
                p_lower = p.lower()
                valid_suffixes = ['jr', 'jr.', 'sr', 'sr.', 'iii', 'iv', 'phd', 'md', 'esq']
                
                # Check if suffix
                is_suffix = False
                if p_lower == "de la": # Legacy special handling
                    is_suffix = True
                elif any(p_lower.startswith(s) for s in valid_suffixes):
                    is_suffix = True
                
                if is_suffix:
                    continue # Strip suffixes (legacy behavior)
                
                # If not a suffix, is it a name?
                if is_likely_name_part(p):
                    kept_parts.append(p)
                # Else it is garbage/title -> Strip
            
            name = ", ".join(kept_parts)

    # Remove " de la" if it's trailing at end of string (special user request)
    if name.strip().lower().endswith(" de la"):
        name = name.strip()[:-6].strip()

    # Ensure we never keep the .timestamped artifact in speaker names
    name = name.replace('.timestamped', '')
    
    # Remove leading/trailing punctuation and whitespace
    name = re.sub(r'^[\s\-:,;\.\'\"]+', '', name)
    name = re.sub(r'[\s\-:,;\.\'\"]+$', '', name)
    
    # Remove leading honorifics (loop to handle stacked prefixes like "Preacher Brother")
    # Added matching for "Bro.Lastname" or "Bro.Firstname" without space (e.g., "Bro.John Smith")
    honorific_prefix_pattern = r'^(?:By|Pr\.?|Br\.?|Bro\.?|Brother|Brothers|Bros\.?|Sister|Sis\.?|Sr\.?|Hna\.?|Hno\.?|Hno|Past\.?|Pastor\.?|Paster\.?|Pastror\.?|Pstr\.?|Ptr\.?|Pst\.?|Preacher|Bishop|Rev\.?|Dr\.?|Evangelist|Apostle|Deacon|Dcn\.?|Guest\s+Minister|Song\s+Leader|Elder|Founding)(?:\s+|(?:(?<=\.)(?=[a-zA-Z])))'
    while True:
        new_name = re.sub(honorific_prefix_pattern, '', name, flags=re.IGNORECASE)
        if new_name == name:
            break
        name = new_name
    
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
    
    # Remove ALL CAPS suffix (likely sermon titles), but preserve short ALL CAPS surnames (e.g. "OVID")
    # Strategy: remove only if the ALL CAPS sequence is long (>5 chars) or multiple words > 3 chars
    name = re.sub(r'\s+[A-Z]{6,}(?:\s+[A-Z]+)*$', '', name) # Remove LONG all caps
    name = re.sub(r'\s+[A-Z]{2,}\s+[A-Z]{2,}\s+[A-Z]{2,}.*$', '', name) # Remove 3+ all caps words sequence
    
    # Remove trailing honorifics (e.g. "Roberto Figueroa Pastor" -> "Roberto Figueroa")
    name = name.strip(" .,:;-|")
    words = name.split()
    trailing_honorifics = {
        'bro', 'bro.', 'brother', 'bros', 'bros.',
        'sis', 'sis.', 'sister',
        'pastor', 'paster', 'pastror', 'pstr', 'pstr.', 'ptr', 'ptr.', 'pst', 'pst.',
        'preacher',
        'hno', 'hno.',
        'rev', 'rev.', 'dr', 'dr.',
        'bishop', 'evangelist', 'apostle', 'elder',
        'deacon', 'dcn', 'dcn.'
    }
    while words:
        last = words[-1].lower().strip(".,:;-|")
        if last in trailing_honorifics:
            words.pop()
            continue
        if last in INVALID_NAME_TERMS:
            words.pop()
            continue
        break
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

    # Canonicalize before matching/adding. This focuses on REPLACE-style cleanup
    # (honorific prefixes/suffixes, whitespace, and .timestamped artifacts).
    speaker = (speaker or '').replace('.timestamped', '').strip()
    speaker = normalize_speaker(speaker)
    speaker = clean_name(speaker)
    speaker = normalize_speaker(speaker)
    speaker = (speaker or '').strip()
    if not speaker or speaker == "Unknown Speaker":
        return "Unknown Speaker"
    # Try to match by first + last name (case-insensitive, ignoring extra spaces)
    def normalize_name(n):
        return " ".join(n.strip().split()).lower()

    def canonical_match(candidate: str):
        cand_norm = normalize_name(candidate)
        for existing in speakers_set:
            if normalize_name(existing) == cand_norm:
                return existing
        return None

    # If the detected speaker looks like "<Known Speaker> <extra words>" (title leakage),
    # collapse to the known canonical speaker. This focuses on REPLACE-style issues and
    # avoids adding polluted variants to speakers.json.
    tokens = speaker.split()
    allowed_suffixes = {'jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv'}
    if len(tokens) >= 3 and tokens[2].lower() not in allowed_suffixes:
        base2 = " ".join(tokens[:2])
        base3 = " ".join(tokens[:3])
        m = canonical_match(base3) or canonical_match(base2)
        if m:
            return m
    speaker_norm = normalize_name(speaker)
    for s in speakers_set:
        if normalize_name(s) == speaker_norm:
            return s  # Return canonical from list
    # If not found, add to speakers.json
    speakers_set.add(speaker)
    save_json_file(speakers_file, speakers_set)
    print(f"[NEW SPEAKER ADDED] '{speaker}' added to {speakers_file}")
    return speaker

def is_valid_person_name(text, title=""):
    """Check if text looks like a valid person name - improved version."""
    if not text or not text.strip():
        return False
    
    text = text.strip()
    t_lower = text.lower()

    # Handle comma-separated multiple speakers
    if "," in text:
        # Check if it matches an exception first (like "art magana, sr.")
        valid_exceptions_check = ["art magaña, sr.", "art magaña, jr.", "art magana, sr", "art magana, jr"] # minimal check before full list
        if t_lower in valid_exceptions_check: 
             pass # Fall through to normal exception check
        else:
             parts = [p.strip() for p in text.split(',') if p.strip()]
             # If it looks like suffix "Name, Jr", clean_name usually handles it. 
             # But if we see it here, check if last part is suffix like "Jr" or "Sr" which is not a name
             if len(parts) > 1 and parts[-1].lower().replace('.','') not in ['jr', 'sr', 'iii', 'iv', 'esq', 'md', 'phd']:
                  return all(is_valid_person_name(p) for p in parts)
    
    # Add specific exceptions for valid names that might otherwise fail
    valid_exceptions = ["bloteh won", "chris take", "tim cross", 
                       "william m. branham", "isiah brooks", "daniel evans", "caleb perez",
                       "art magaña", "art magana", "art magaña, sr.", "art magaña, jr.", "art magana, sr", "art magana, jr",
                       "it mojolabe", "katumba james", "meryl kinkonda", "martin shalom", "diego cantos", "juan espinoza juancho"]
    if t_lower in valid_exceptions:
        return True

    # Explicit non-speaker group/organization names
    if t_lower in {"church choir", "circuit riders", "ciruit riders"}:
        return False

    # Explicit non-speaker titles that have leaked into the speaker field
    title_list = {
        "i",
        "redemption", "intellectual", "operating", "bethel", "poured", "benefits", "purchased", "optics", "gentile", "fatherhood", "gratitude", "champion", "rapturing", "composed", "argentina", "coordination", "presuming", "artificial", "becoming", "maintain", "amazing", "falling", "expression", "expectation", "pressurized", "merrily", "surrounded", "perseverance", "daystar", "valley", "furlong", "adoption", "greatest", "security", "perplexity", "respeto", "brotherly", "emotional", "eternity", "original", "eating", "hungry", "influenced", "prospering", "moment", "established", "appropriate", "unaltered", "tested", "return", "opportunity", "priesthood", "nobody", "highway", "apostolic", "conduct", "fathers", "breathe", "harvest", "thankful", "predestinated", "nothing", "happiness", "atmospheres", "freewill", "prosper", "revelation", "enquiring", "byfaith", "prepared", "testing", "proving", "lifted", "misunderstanding", "multiplying", "washed", "accountability", "silence", "melody", "bloodline", "gethsemane", "remember", "witness", "messianic", "pruning", "watchnight", "eternal", "forgive", "emergency", "emotions", "caught", "fiftieth", "reformation", "credits", "representation", "serpent", "earnestly", "recover", "sealed", "friendship", "sacrifice", "climbing", "transformation", "unhindered", "trained", "anchored", "determination", "casting", "traits", "gratefulness", "elevation", "junctions", "impossible", "victories", "citizens", "comfort", "conflicting", "remembering", "theomorphy", "redeemable", "untapped", "settle", "impressions", "unwrapped", "uncentered", "unglazed", "unwaxed", "sonlit", "brilliant", "poison", "sympathetic", "musings", "wasteland", "relevant", "relatable", "marked", "framed", "antifaith", "integrity", "ruined", "fashioned", "deceptive", "ugliness", "sacred", "accepting", "complicated", "example", "ministered", "restitution", "groaning", "ruling", "conditions", "privileged", "refined", "measured", "sifted", "ambassadors", "courtroom", "thankfulness", "changes", "philemon", "gentle", "embracing", "influences", "loneliness", "switching", "trapped", "passing", "reaction", "examination", "purifying", "feeling", "maintaining", "inspired", "parenthood", "responsibility", "bitterness", "communtion", "dressed", "peacemakers", "untitled", "weapon", "prince", "nevertheless", "changer", "stages", "questioning", "reunited", "raised", "managing", "smaller", "literal", "available", "pulling", "strange", "glorious", "performed", "lovely", "evidence", "witnessing", "flight", "partakers", "hindrances", "endure", "examples", "impregnated", "leadership", "beneficiaries", "eyewitness", "messiahettes", "somebody", "inoculated", "masterbuilder", "fallen", "graves", "scatter", "protection", "anticipating", "ordination", "shaped", "quickened", "homecoming", "business", "according", "podcast", "prison", "influence", "paradox", "predestination", "constant", "learning", "unbreakable", "weaponizing", "necesito", "letting", "lukewarm", "siempre", "obedience", "declarando", "apocalipsis", "uniting", "watchman", "conditioning", "horses", "waymaker", "identification", "cherish", "mighty", "further", "rescue", "newness", "belief", "refiners", "justified", "staying", "unveiled", "remembered", "exhorted", "samson", "season", "manifesting", "stimulation", "cursed", "undisturbed", "finishers", "dangerous", "favored", "exchange", "condemnation", "strong", "consistently", "chasing", "working", "promises", "abundance", "believe", "exalted", "speechless", "impacted", "reversal", "covenant", "pursuing", "worshipping", "familiar", "functioning", "introduction", "mistakes", "annual", "excerpt", "lovest", "charity", "temperance", "virtue", "forgiving", "secret", "remaining", "engulf", "abounding", "favour", "hungering", "exceptional", "discernment", "cheerful", "appetite", "heeled", "effective", "battling",
        "conferencia con dios",
        "confronting eternal",
        "correctly overcoming",
        "corriendo con paciencia",
        "cosmetic christians",
        "covenanted blood purging",
        "creating peace",
        "crossing jordan",
        "crying grace",
        "dark places",
        "empty vessels",
        "encourage yourself",
        "end time evangelism",
        "end-time transfromation",
        "enfocando nuestros pensamientos",
        "enlarging territories",
        "elohim operating",
        "elt adult",
        "embracing deity",
        "entertaining strangers",
        "eternal plan",
        "eternal purpose",
        "eternal redemption",
        "eternal separation",
        "etm tabernacles crossover",
        "everlasting consequences",
        "everlasting covenant",
        "exceeding righteousness",
        "except ye abide",
        "exceptional praise",
        "experiencing fulfillment",
        "experiencing gods",
        "extra chair",
        "face-to-face relationship",
        "faint yet pursuing",
        "faiths reaction",
        "false anointed ones",
        "false humility",
        "familiar places",
        "familiarity breeds contempt",
        "family idols",
        "family matters overview",
        "family positions",
        "family working together",
        "forsaken then crowned",
        "abstract title deed",
        "ill fly away",
        "i'll fly away",
        "infant seeds",
        "invasion insanity deliverance",
        "invert always invert",
        "investigating angels",
        "humble thyself",
        "humble yourself",
        "israel commemoration",
        "israel gods redemption",
        "ive been changed",
        "i've been changed",
        "ive tried",
        "i've tried",
        "only believe",
        "open channels",
        "open door",
        "original inspiration",
        "original sin",
        "josephs silver cup",
        "joshua parallels ephesians",
        "judgement seat",
        "lump without leaven",
        "peter skhosana date",
        "peter skosana title",
        "peter skosana venue",
        "phoenix lighthouse",
        "picture perfect",
        "piezas de rompecabezas",
        "plan possess plant",
        "precious promises",
        "prepared ground",
        "present context",
        "present darkness",
        "present tense manifestation",
        "present tense revelation",
        "promise island",
        "prophecy politics",
        "prosper lamour parfait",
        "pure heart",
        "rapture landscape",
        "rapturing conditin",
        "rapturing strength",
        "rapturing without",
        "rejected stone",
        "religion versus relationship",
        "remaining fruitful",
        "remaining vigilant",
        "remembering calvary",
        "remembering gods gift",
        "rescue story brother",
        "resolving evil",
        "respecting gods",
        "revelacion espiritual",
        "revelation brings strength",
        "revelation resolves complication",
        "rich young ruler",
        "right hand",
        "right now",
        "righteous mans reward",
    }
    if t_lower in title_list:
        return False
    
    # Check against NON_NAME_PATTERNS
    for pattern in NON_NAME_PATTERNS:
        if re.match(pattern, text, re.IGNORECASE):
            return False
    
    # Reject obvious junk
    if t_lower.startswith(("the ", "a ", "an ", "i ", "my ", "if ", "this ", "that ", "when ", "where ", "what ", "how ", "why ",
                           "in ", "on ", "at ", "to ", "for ", "by ", "with ")): 
        return False
    if t_lower.endswith((" the", " a", " is", " are", " was", " be", " in", " on", " at", " to", " for", " by", " with", " and", " or")):
        return False
    
    # Reject names containing " and " without honorifics (mirroring final_validation)
    if " and " in t_lower or " & " in t_lower:
         if not any(h in t_lower for h in ["bro", "sis", "pas", "rev", "bish", "eld"]):
              # Simple heuristic rejection for single word matches e.g. "Bread and Wine"
              if len(t_lower.split()) < 4: 
                  return False
    
    # Reject service/content words
    service_words = ["hymn", "service", "sermon", "worship", "meeting", "prayer", "song", 
                     "baptism", "dedication", "funeral", "memorial", "testimony", "testimonies",
                     "communion", "supper", "revival", "conference", "camp"]
    for word in service_words:
        if re.search(r'\b' + re.escape(word) + r'\b', t_lower):
            return False
    
    # Reject topic keywords
    topic_keywords = ["how", "why", "when", "where", "what", "should", "must", "will",
                      "shall", "being", "having", "taking", "making", "getting", "going"]
    for word in topic_keywords:
        if t_lower.startswith(word + " ") or t_lower.endswith(" " + word):
            return False

    # CRITICAL: Reject names with digits (dates, years, numbers)
    if any(char.isdigit() for char in text):
        return False

    # CRITICAL: Reject dates using broad patterns
    if re.search(r'\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\b', t_lower):
        return False

    # --- ROUND 9 AUDIT ADDITIONS ---

    # Reject time-based patterns (Sunday Morning, Wednesday Evening, etc.)
    if re.match(r'^(sunday|monday|tuesday|wednesday|thursday|friday|saturday)\s+(morning|evening|night|service)', t_lower):
        return False

    # Reject single words that are in CATEGORY_TITLES
    if text in CATEGORY_TITLES or text.title() in CATEGORY_TITLES:
        return False

    # Reject Part/Pt numbers (sermon series)
    if re.search(r'\b(pt\.?|part)\s*\d', t_lower):
        return False

    # Reject speakers starting with articles (sermon titles)
    if re.match(r'^(the|a|an)\s+', t_lower):
        return False

    # Reject gerund + preposition patterns (sermon titles)
    if re.match(r'^[a-z]+ing\s+(the|in|to|of|with|on|a|an|for|at|by)\s+', t_lower):
        return False

    # Reject "X Of Y" patterns (sermon titles) unless it looks like a name
    if re.search(r'\s+of\s+[a-z]', t_lower):
        words = text.split()
        # Allow "Juan Carlos of Mexico" style but reject "Token Of Life"
        if len(words) < 2 or words[0].lower() in INVALID_NAME_TERMS:
            return False

    # Reject speakers with parentheses
    if '(' in text or ')' in text:
        return False

    # Reject incomplete/fragment names
    incomplete_names = {'de la', 'william marrion', 'h simmons', 'apostle bernie'}
    if t_lower in incomplete_names:
        return False

    # --- END ROUND 9 AUDIT ADDITIONS ---

    # Check invalid terms
    text_words = t_lower.split()
    for word in text_words:
        w_clean = word.strip(".,:;-")
        if w_clean in INVALID_NAME_TERMS:
             # Special exception for "Jesus" if it is part of a name (e.g. Jesus Rendon)
             # But reject "Jesus Christ", "Lord Jesus", "Jesus" alone
             if w_clean == 'jesus' and len(text_words) > 1:
                  if any(x in t_lower for x in ['christ', 'lord', 'god', 'king', 'saviour', 'savior']):
                       return False
                  continue
             return False
    
    # Must have reasonable word count (1-5 words typical for names, detecting "Ben", "Art")
    words = text.split()
    if not (1 <= len(words) <= 5):
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
    Attempts to fix "Bad Speakers" using extensive normalization rules from config
    and pattern matching logic.
    """
    # Load configuration 
    try:
        config_data = load_json_file(SPEAKERS_CONFIG_FILE)
        normalization_rules = config_data.get("normalization_rules", {})
    except:
        normalization_rules = {}

    # 1. Clean basic prefixes
    clean = clean_name(current_speaker)
    norm = normalize_speaker(clean)
    
    # 2. Apply Config-Driven Normalization Rules
    if norm in normalization_rules:
        return normalization_rules[norm]
        
    # 3. Check for "Name, Title" pattern. Be less aggressive.
    # If a comma is present, trust that it's separating speakers and just clean them.
    if "," in norm:
        parts = split_multiple_speakers(norm)
        cleaned_parts = [clean_name(p) for p in parts]
        return ", ".join(cleaned_parts)

    # 4. Check for "Title as Speaker" pattern
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

# Load UNWANTED_SPEAKERS from JSON Config
UNWANTED_SPEAKERS = set()
try:
    _sp_config = load_json_file(SPEAKERS_CONFIG_FILE)
    if _sp_config and "invalid_speakers" in _sp_config:
        UNWANTED_SPEAKERS = set(_sp_config["invalid_speakers"])
except Exception as e:
    print(f"⚠️ Warning: Could not load invalid_speakers from {SPEAKERS_CONFIG_FILE}: {e}")
    # Fallback/Default set only if config fails significantly
    UNWANTED_SPEAKERS = set([
        "Unknown Speaker", "Guest Speaker", "Various Speakers", 
        "Song Service", "Testimony", "Prayer", "Worship"
    ])

def heal_archive(data_dir, force=False, churches=None):
    print("\n" + "="*60)
    print("🚑 STARTING DEEP ARCHIVE HEALING & CLEANUP")
    print(f"heal_archive called with data_dir={data_dir}, force={force}")
    if churches:
        print(f"   🎯 Filtering to {len(churches)} church(es)")
    if force:
        print("   ⚠️ FORCE MODE: Re-processing all entries.")
    print("="*60)
    print("About to iterate church folders...")
    
    speakers_before_set = load_json_file(SPEAKERS_FILE)
    updated_files_count = 0
    cleaned_speakers = set()
    speaker_corrections_log_rows = []
    csv_files_processed = []
    
    # Load known speakers for full speaker detection
    known_speakers = load_json_file(SPEAKERS_FILE)
    known_casefold = build_known_speakers_casefold_map(known_speakers)
    
    # Statistics tracking for speaker detection
    heal_stats = {
        'total_processed': 0,
        'speakers_detected': 0,
        'unknown_speakers': 0,
        'unknown_speakers_before': 0,
        'speakers_redetected': 0,
        'speakers_corrected': 0,
        'new_speakers': set(),
        'by_church': {}
    }
    # Shadow master summary file for detected speakers
    SHADOW_MASTER_FILE = os.path.join(data_dir, "shadow_master_speakers.csv")
    shadow_rows = []
    shadow_header = ["speaker_name", "source", "detected_date", "notes"]
    
    allowed_churches = None
    if churches:
        allowed_churches = {c.replace(' ', '_').casefold() for c in churches if c}

    # 1. Iterate over every Church Folder
    for church_folder in os.listdir(data_dir):
        church_path = os.path.join(data_dir, church_folder)
        if not os.path.isdir(church_path): continue

        if allowed_churches is not None and church_folder.casefold() not in allowed_churches:
            continue
        
        summary_path = os.path.join(data_dir, f"{church_folder}_Summary.csv")
        csv_files_processed.append(summary_path)
        if not os.path.exists(summary_path): continue
        
        print(f"   🏥 Healing: {church_folder.replace('_', ' ')}...")
        
        # Initialize per-church stats
        church_stats = {'total': 0, 'detected': 0, 'unknown': 0}
        
        new_rows = []
        headers = []
        
        try:
            with open(summary_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                headers = list(reader.fieldnames) if reader.fieldnames else []
                if "church" not in headers:
                    headers.append("church")
                rows = list(reader)
        except: continue
        
        # Build a set of (date, title, speaker) from current summary for fast lookup
        summary_keys = set()
        for row in rows:
            key = (row.get('date', '').strip(), row.get('title', '').strip(), row.get('speaker', '').strip())
            summary_keys.add(key)

        # --- NEW LOGIC: Ensure every .txt transcript is represented in the summary CSV ---
        txt_files = [f for f in os.listdir(church_path) if f.endswith('.txt') and not f.endswith('.timestamped.txt')]
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
                    "type": video_type,
                    "church": church_folder.replace('_', ' ')
                }
                rows.append(new_row)
                summary_keys.add((date, title, speaker))
                updated_files_count += 1
            else:
                print(f"      ✔ Already in summary: {txt_file}")

        for row in rows:
            # Ensure church field is populated
            if "church" not in row or not row["church"]:
                row["church"] = church_folder.replace('_', ' ')

            original_speaker = row.get('speaker', 'Unknown Speaker')
            raw_original_speaker = original_speaker
            speaker_reason = ""
            
            # Track stats
            church_stats['total'] += 1
            heal_stats['total_processed'] += 1
            if not original_speaker or original_speaker.strip() == "" or original_speaker.strip() == "Unknown Speaker":
                heal_stats['unknown_speakers_before'] += 1
            
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
                speaker_reason = "unwanted_speaker"
            

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
                if new_speaker != original_speaker:
                    speaker_reason = speaker_reason or "smart_correction"

            # --- VALIDITY CHECK (Applies expanded blocklists) ---
            if new_speaker != "Unknown Speaker" and not is_valid_person_name(new_speaker):
                 # print(f"      - Invalidated speaker: '{new_speaker}' -> 'Unknown Speaker'")
                 new_speaker = "Unknown Speaker"
                 speaker_reason = "invalid_name_check"

            # --- STEP 1.5: FULL SPEAKER DETECTION FOR UNKNOWN SPEAKERS ---
            # If speaker is still unknown after smart correction, run full detection algorithm
            if new_speaker == "Unknown Speaker" or not new_speaker:
                description = row.get('description', '')
                detected_speaker, is_new = identify_speaker_dynamic(original_title, description, known_speakers, date_str=original_date)
                detected_speaker = normalize_speaker(detected_speaker)
                detected_speaker = clean_name(detected_speaker)
                
                if detected_speaker and detected_speaker != "Unknown Speaker":
                    new_speaker = detected_speaker
                    heal_stats['speakers_redetected'] += 1
                    print(f"      🔍 DETECTED: '{original_title[:40]}...' -> {new_speaker}")
                    speaker_reason = speaker_reason or "redetected"
                    if is_new:
                        key = speaker_casefold_key(new_speaker)
                        if key and key not in known_casefold:
                            heal_stats['new_speakers'].add(new_speaker)
                            known_speakers.add(new_speaker)
                            known_casefold[key] = new_speaker
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

            # Exclude certain titles from Song / Worship classification even if content looks song-like
            # These are services that may have short musical segments but aren't primarily songs
            title_lower = original_title.lower()
            song_exclusion_patterns = [
                "live service", "sermon clip", "homegoing", "home going",
                "memorial", "funeral", "dedication service", "baptism", "communion",
                "wedding", "tribute", "testimony", "testimonies",
                "youth camp", "youth retreat", "tent revival", "convention", "conference",
                "bethel stream", "live stream"
            ]
            is_excluded_from_song = any(pattern in title_lower for pattern in song_exclusion_patterns)

            if is_song and not is_excluded_from_song:
                new_type = "Song / Worship"
            elif "choir" in new_speaker.lower():
                new_type = "Choir"
            else:
                # --- STEP 2.5: RE-EVALUATE CATEGORY ---
                duration_minutes = 0
                try: 
                    # CSV duration is stored in minutes
                    duration_minutes = float(row.get('duration', 0))
                except: pass
                
                # If duration missing in CSV, check if we have a file to estimate from
                if duration_minutes == 0 and os.path.exists(old_filepath):
                    try:
                        with open(old_filepath, 'r', encoding='utf-8') as f:
                            # Quick check for timestamped file structure
                            first_chars = f.read(50)
                            if '<' in first_chars and '>' in first_chars: # Simplistic check for timestamp tags
                                # It's timestamped, this is hard to parse quickly without reading whole file
                                # Let's assume average file size correlation or just skip
                                pass
                            else:
                                # Dictionary method: crude word count estimate
                                f.seek(0, 2) # Seek end
                                size = f.tell()
                                # Avg 130 wpm, approx 5 chars per word + space = 6 bytes/word.
                                # 130 wpm * 6 = 780 bytes/min.
                                duration_minutes = size / 780.0
                                # Save estimated duration to row
                                row['duration'] = int(duration_minutes)
                    except: pass
                
                description = row.get('description', '')

                # Create a mock object with duration for determine_video_type
                class MockYT:
                    def __init__(self, dur_min):
                        self.length = dur_min * 60 if dur_min else 0
                mock_yt = MockYT(duration_minutes) if duration_minutes else None

                recalc_type = determine_video_type(original_title, new_speaker, None, mock_yt, description)
                
                # Logic to preserve manual "Short Clip" if we don't know duration
                if recalc_type == "Church Service" and duration_minutes == 0:
                    if original_type in ["Short Clip", "Short Service"]:
                        new_type = "Short Clip"
                    else:
                        new_type = "Church Service" # Upgrade "Full Sermon" -> "Church Service"
                else:
                    new_type = recalc_type

            # --- MANUAL OVERRIDES (one-off corrections) ---
            new_speaker, new_type = apply_manual_metadata_overrides(original_title, new_speaker, new_type)
                
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

            from_s = (raw_original_speaker or '').strip()
            to_s = (canonical_speaker or '').strip()
            if from_s != to_s:
                speaker_corrections_log_rows.append({
                    "church": church_folder.replace('_', ' '),
                    "date": (original_date or '').strip(),
                    "title": (original_title or '').strip(),
                    "url": (row.get('url', '') or '').strip(),
                    "from_speaker": from_s,
                    "to_speaker": to_s,
                    "reason": speaker_reason or "healed",
                })
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

    # Log speaker inventory deltas (speakers.json) + which CSVs were processed
    heal_stats.update(compute_speaker_inventory_delta(speakers_before_set, final_speakers))
    heal_stats['csv_files_processed'] = [p for p in csv_files_processed if p]
    heal_stats['speakers_changed_to_unknown'] = sum(
        1 for r in speaker_corrections_log_rows
        if (r.get('to_speaker') == 'Unknown Speaker') and (r.get('from_speaker') != 'Unknown Speaker')
    )

    # Write detailed speaker FROM/TO logs for this healing run
    detailed_csv, summary_csv = write_heal_speaker_corrections_logs(
        speaker_corrections_log_rows,
        data_dir=data_dir,
        operation_name="Heal Archive",
    )
    if detailed_csv or summary_csv:
        print(f"🧾 Speaker corrections recorded: {len(speaker_corrections_log_rows)}")
        if detailed_csv:
            print(f"   📄 Detailed log: {detailed_csv}")
        if summary_csv:
            print(f"   📄 FROM/TO summary: {summary_csv}")

    heal_stats['speakers_corrected'] = len(speaker_corrections_log_rows)
    
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

    speakers_before_set = load_json_file(SPEAKERS_FILE)
    csv_files_processed = [csv_path]
    
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
        csv_files_processed.append(summary_path)
        
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
                fieldnames = ["date", "status", "speaker", "title", "url", "last_checked", "language", "type", "description", "duration", "church", "first_scraped", "video_status", "video_removed_date"]
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
    known_casefold = {speaker_casefold_key(s): s for s in known_speakers}
    for correction in corrections:
        new_speaker = correction.get('speaker_detected', '').strip()
        if new_speaker and new_speaker != "Unknown Speaker":
            new_speaker = normalize_speaker(new_speaker)
            new_speaker = clean_name(new_speaker)
            if new_speaker and is_valid_person_name(new_speaker):
                key = speaker_casefold_key(new_speaker)
                if key not in known_casefold:
                    new_speakers_added.add(new_speaker)
                    known_speakers.add(new_speaker)
                    known_casefold[key] = new_speaker
    save_json_file(SPEAKERS_FILE, known_speakers)
    speakers_after_set = load_json_file(SPEAKERS_FILE)
    
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
        'speakers_changed_to_unknown': 0,
        'csv_files_processed': [p for p in csv_files_processed if p],
    }
    stats.update(compute_speaker_inventory_delta(speakers_before_set, speakers_after_set))
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
    Backfill timestamped transcript data for existing .txt files that are missing .timestamped.txt.
    
    This function:
    1. Scans all transcript .txt files in data_dir
    2. Checks if each file has a corresponding .timestamped.txt
    3. If missing, extracts the video ID from the URL in the file
    4. Fetches the caption XML from YouTube
    5. Parses timestamps and creates the .timestamped.txt file
    
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
    
    # Count total .txt files that actually need timestamped siblings for progress
    total_to_process = 0
    for church_folder in church_folders:
        church_path = os.path.join(data_dir, church_folder)
        if not os.path.isdir(church_path):
            continue
        if church_folder.startswith('.') or church_folder.endswith('.csv'):
            continue
        for filename in os.listdir(church_path):
            if not filename.endswith('.txt') or filename.endswith('.timestamped.txt'):
                continue
            txt_path = os.path.join(church_path, filename)
            timestamped_path = txt_path.replace('.txt', '.timestamped.txt')
            if not os.path.exists(timestamped_path):
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
            fieldnames = ["date", "status", "speaker", "title", "url", "last_checked", "language", "type", "description", "duration", "church", "first_scraped", "video_status", "video_removed_date"]
            
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


def backfill_duration_metadata(data_dir, dry_run=False, churches=None, limit=None, force_all=False):
    """
    Scrape duration metadata for videos in CSV summary files.
    """
    print("\n" + "="*60)
    print("⏱️ BACKFILLING VIDEO DURATION METADATA")
    if force_all:
        print("   ⚠️ FORCE MODE: Checking ALL videos, even if duration exists")
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
        
        # --- Pre-scan filenames to map Video IDs -> Filepaths ---
        # This resolves the issue where constructed filenames don't match actual files on disk
        church_folder_name = csv_file.replace('_Summary.csv', '')
        church_dir = os.path.join(data_dir, church_folder_name)
        video_id_to_file_map = {}
        
        if os.path.exists(church_dir) and os.path.isdir(church_dir):
            print(f"   🔎 Building file map for {church_folder_name}...")
            for fname in os.listdir(church_dir):
                if fname.endswith('.txt') and not fname.endswith('.timestamped.txt'):
                    fpath = os.path.join(church_dir, fname)
                    try:
                        with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                            # Read first 50 lines to find URL or ID
                            head = [next(f) for _ in range(50)]
                            content_head = "".join(head)
                            
                            # Extract ID
                            vid_match = re.search(r'(?:v=|youtu\.be/|vi/)([\w\-]{11})', content_head)
                            if vid_match:
                                vid = vid_match.group(1)
                                video_id_to_file_map[vid] = fpath
                    except StopIteration:
                        pass # empty file
                    except Exception as e:
                        pass # read error
            print(f"   🗺️ Mapped {len(video_id_to_file_map)} transcript files")
        else:
            print(f"   ⚠️ Church directory not found: {church_dir}")

        try:
            # Read existing CSV
            rows = []
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            # Find entries to process
            items_to_process = []
            for i, row in enumerate(rows):
                duration = row.get('duration', '')
                is_missing = not duration or duration == '0' or duration == ''
                
                if force_all or is_missing:
                    url = row.get('url', '')
                    if url and 'watch?v=' in url:
                        items_to_process.append((i, row))
            
            if not items_to_process:
                print(f"   ✓ No applicable entries found (Force Mode: {force_all})")
                total_skipped += len(rows)
                continue
            
            print(f"   📊 Found {len(items_to_process)} entries to check")
            
            # Apply limit
            to_process = items_to_process[:limit] if limit else items_to_process
            
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
                    yt_obj = youtube_with_timeout(url, use_oauth=False, allow_oauth_cache=True)
                    if yt_obj is None:
                        print(f"      ❌ Skipped (timeout)")
                        total_errors += 1
                        continue
                    duration_seconds = yt_obj.length if hasattr(yt_obj, 'length') else 0
                    duration_minutes = int(duration_seconds / 60) if duration_seconds else 0
                    
                    # Format as standard time string (H:M)
                    hours = int(duration_minutes // 60)
                    mins = int(duration_minutes % 60)
                    if hours > 0:
                        duration_fmt = f"{hours}h {mins}m"
                    else:
                        duration_fmt = f"{mins}m"
                    
                    rows[row_idx]['duration'] = duration_minutes
                    print(f"      ✅ Duration: {duration_fmt}")
                    updated_in_church += 1

                    # --- AUTOMATIC HEALING: Transcript Header & Category ---
                    if not dry_run:
                        # Re-evaluate Video Type based on new duration
                        orig_type = row.get('type', 'Full Sermon').strip()
                        orig_date = row.get('date', '').strip()
                        orig_title = row.get('title', '').strip()
                        orig_speaker = row.get('speaker', '').strip()
                        desc = row.get('description', '')

                        # Create a mock object with duration for determine_video_type
                        class MockYT:
                            def __init__(self, dur_min):
                                self.length = dur_min * 60 if dur_min else 0
                        mock_yt = MockYT(duration_minutes) if duration_minutes else None

                        recalc_type = determine_video_type(orig_title, orig_speaker, None, mock_yt, desc)
                        
                        # Apply specific logic overrides
                        new_type = orig_type
                        if recalc_type == "Church Service" and duration_minutes > 0:
                            if orig_type in ["Short Clip", "Short Service"]:
                                new_type = "Short Clip"  # Trust manual short clip labels
                            else:
                                new_type = "Church Service"
                        else:
                            new_type = recalc_type
                        
                        if new_type != orig_type:
                            rows[row_idx]['type'] = new_type
                            print(f"      🔄 Healed Type: {orig_type} -> {new_type}")

                        # Update Transcript File Header
                        # Use the pre-built map to find the actual file on disk
                        filepath = video_id_to_file_map.get(video_id)
                        
                        if filepath and os.path.exists(filepath):
                            try:
                                with open(filepath, 'r', encoding='utf-8') as tf:
                                    content = tf.read()
                                
                                # 1. Update/Add Duration
                                if "Duration:" in content:
                                    content = re.sub(r'Duration:.*', f'Duration: {duration_fmt}', content)
                                else:
                                    # Insert before Language or Type (common headers)
                                    insert_marker = "Language:"
                                    if insert_marker in content:
                                        content = content.replace(insert_marker, f"Duration: {duration_fmt}\n{insert_marker}")
                                    else:
                                        # Fallback: Insert after Start of File line if present, else top
                                        lines = content.split('\n')
                                        if len(lines) > 3:
                                            lines.insert(3, f"Duration: {duration_fmt}")
                                            content = '\n'.join(lines)
                                
                                # 2. Update Type if changed
                                if new_type != orig_type:
                                    content = re.sub(r'Type:.*', f'Type:    {new_type}', content)
                                
                                with open(filepath, 'w', encoding='utf-8') as tf:
                                    tf.write(content)
                            except Exception as th_err:
                                print(f"      ⚠️ Failed to update transcript header: {th_err}")
                    
                except Exception as e:
                    print(f"      ❌ Error: {str(e)[:40]}")
                    total_errors += 1
            
            # Write updated CSV
            if not dry_run and updated_in_church > 0:
                fieldnames = ["date", "status", "speaker", "title", "url", "last_checked", "language", "type", "description", "duration", "church", "first_scraped", "video_status", "video_removed_date"]
                try:
                    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                        writer.writeheader()
                        writer.writerows(rows)
                    print(f"   📝 Saved {updated_in_church} duration updates to CSV")
                except Exception as save_err:
                    print(f"   ❌ Error saving CSV: {save_err}")
            
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


def heal_unknown_dates(data_dir, dry_run=False, churches=None, limit=None):
    """
    Fix 'Unknown Date' entries in Summary CSV files by fetching YouTube publish dates.
    Uses pytubefix to get the publish_date from each video URL.
    """
    print("\n" + "="*60)
    print("📅 HEALING UNKNOWN DATES")
    if dry_run:
        print("   🔍 DRY RUN MODE - No changes will be made")
    if churches:
        print(f"   📌 Limited to churches: {churches}")
    if limit:
        print(f"   📊 Limit: {limit} entries")
    print("="*60)
    
    # Parse churches filter
    church_filter = None
    if churches:
        church_filter = [c.strip().replace(' ', '_') for c in churches.split(',')]
    
    total_found = 0
    total_fixed = 0
    total_failed = 0
    processed = 0
    
    # Find all Summary CSV files
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('_Summary.csv')]
    
    for csv_file in sorted(csv_files):
        if limit and processed >= limit:
            break
            
        church_name = csv_file.rsplit('_Summary.csv', 1)[0]
        
        # Apply church filter
        if church_filter and church_name not in church_filter:
            continue
        
        csv_path = os.path.join(data_dir, csv_file)
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames
                rows = list(reader)
        except Exception as e:
            print(f"   ⚠️ Error reading {csv_file}: {e}")
            continue
        
        # Find entries with Unknown Date that have a video URL
        unknown_entries = []
        for i, row in enumerate(rows):
            if row.get('date', '').strip() in ['Unknown Date', 'Unknown', '']:
                url = row.get('url', '').strip()
                if url and ('youtube.com/watch' in url or 'youtu.be/' in url):
                    unknown_entries.append((i, row))
        
        if not unknown_entries:
            continue
        
        print(f"\n   🏥 {church_name.replace('_', ' ')}: {len(unknown_entries)} Unknown Date entries")
        total_found += len(unknown_entries)
        
        modified = False
        for idx, (row_idx, row) in enumerate(unknown_entries):
            if limit and processed >= limit:
                break
            processed += 1
            
            title = row.get('title', 'Unknown')[:50]
            url = row.get('url', '')
            
            print(f"      [{idx+1}/{len(unknown_entries)}] {title}...")
            
            if dry_run:
                print(f"         Would fetch date from: {url}")
                continue
            
            try:
                time.sleep(1)  # Rate limiting
                yt_obj = youtube_with_timeout(url, use_oauth=False, allow_oauth_cache=True)
                if yt_obj is None:
                    total_failed += 1
                    continue

                # Try to get date from title/description first
                description = yt_obj.description or ""
                new_date = determine_sermon_date(title, description, yt_obj)

                if new_date and new_date != "Unknown Date":
                    rows[row_idx]['date'] = new_date
                    modified = True
                    total_fixed += 1
                    print(f"         ✅ Fixed: {new_date}")
                else:
                    # Last resort: use publish_date directly
                    if yt_obj.publish_date:
                        new_date = yt_obj.publish_date.strftime("%Y-%m-%d")
                        rows[row_idx]['date'] = new_date
                        modified = True
                        total_fixed += 1
                        print(f"         ✅ Fixed (publish date): {new_date}")
                    else:
                        total_failed += 1
                        print(f"         ❌ Could not determine date")

            except Exception as e:
                total_failed += 1
                print(f"         ❌ Error: {str(e)[:50]}")
        
        # Write back the CSV if modified
        if modified and not dry_run:
            try:
                with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    writer.writerows(rows)
                print(f"      💾 Saved {csv_file}")
            except Exception as e:
                print(f"      ❌ Error saving {csv_file}: {e}")
    
    print("\n" + "="*60)
    print("📅 UNKNOWN DATES HEALING COMPLETE")
    print(f"   📊 Found: {total_found} entries with Unknown Date")
    print(f"   ✅ Fixed: {total_fixed}")
    print(f"   ❌ Failed: {total_failed}")
    print("="*60)
    
    return total_fixed, total_failed


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
                    fieldnames = ["date", "status", "speaker", "title", "url", "last_checked", "language", "type", "description", "duration", "church", "first_scraped", "video_status", "video_removed_date"]
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


def recover_unknown_speakers(data_dir, dry_run=False, churches=None, limit=None):
    """
    Re-run speaker detection on entries with 'Unknown Speaker' to recover
    speakers using improved detection algorithms.

    This function:
    1. Finds all entries with 'Unknown Speaker'
    2. Re-runs speaker detection using title and description
    3. Updates only if a valid speaker is found
    4. Renames transcript files to match new speaker
    5. Reports statistics on recovered speakers
    """
    print("\n" + "="*60)
    print("🔍 RECOVERING UNKNOWN SPEAKERS")
    if dry_run:
        print("   🔍 DRY RUN MODE - No changes will be made")
    if churches:
        print(f"   📂 Filtering to churches: {churches}")
    if limit:
        print(f"   📊 Limit: {limit} entries")
    print("="*60)

    # Load known speakers
    known_speakers = load_json_file(SPEAKERS_FILE) or set()
    if isinstance(known_speakers, list):
        known_speakers = set(known_speakers)

    csv_files = [f for f in os.listdir(data_dir) if f.endswith('_Summary.csv')]

    total_unknown = 0
    total_recovered = 0
    total_still_unknown = 0
    recovered_speakers = {}  # Track which speakers were recovered and how many
    processed_count = 0

    for csv_file in sorted(csv_files):
        church_name = csv_file.replace('_Summary.csv', '').replace('_', ' ')
        church_folder = csv_file.replace('_Summary.csv', '')
        church_path = os.path.join(data_dir, church_folder)

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

            recovered_in_church = 0
            unknown_in_church = 0
            modified = False

            for row in rows:
                speaker = row.get('speaker', 'Unknown Speaker').strip()

                # Only process Unknown Speaker entries
                if speaker != 'Unknown Speaker':
                    continue

                # Check limit
                if limit and processed_count >= limit:
                    break

                unknown_in_church += 1
                total_unknown += 1
                processed_count += 1

                title = row.get('title', '')
                description = row.get('description', '')
                date = row.get('date', '')

                # Try to detect speaker
                detected_speaker, _ = identify_speaker_dynamic(title, description, known_speakers, date_str=date)

                if detected_speaker and detected_speaker != 'Unknown Speaker':
                    # Validate and clean
                    detected_speaker = normalize_speaker(detected_speaker, title)
                    detected_speaker = clean_name(detected_speaker)
                    validated = final_validation(detected_speaker, title)

                    if validated and validated != 'Unknown Speaker':
                        # Successfully recovered a speaker!
                        if not dry_run:
                            # Rename transcript file if it exists
                            old_safe_title = sanitize_filename(title)
                            old_filename = f"{date} - {old_safe_title} - Unknown Speaker.txt"
                            old_filepath = os.path.join(church_path, old_filename)

                            if os.path.exists(old_filepath):
                                new_safe_speaker = sanitize_filename(validated)
                                new_filename = f"{date} - {old_safe_title} - {new_safe_speaker}.txt"
                                new_filepath = os.path.join(church_path, new_filename)

                                try:
                                    # Also update header in transcript file
                                    with open(old_filepath, 'r', encoding='utf-8') as f:
                                        content = f.read()

                                    # Update header if present
                                    if 'Speaker: Unknown Speaker' in content:
                                        content = content.replace('Speaker: Unknown Speaker', f'Speaker: {validated}')
                                        with open(old_filepath, 'w', encoding='utf-8') as f:
                                            f.write(content)

                                    # Rename file
                                    os.rename(old_filepath, new_filepath)

                                    # Also handle timestamped file if exists
                                    old_ts = old_filepath.replace('.txt', '.timestamped.txt')
                                    new_ts = new_filepath.replace('.txt', '.timestamped.txt')
                                    if os.path.exists(old_ts):
                                        os.rename(old_ts, new_ts)
                                except Exception as e:
                                    print(f"      ⚠️ Error renaming file: {e}")

                            row['speaker'] = validated
                            modified = True

                        recovered_in_church += 1
                        total_recovered += 1
                        recovered_speakers[validated] = recovered_speakers.get(validated, 0) + 1

                        if dry_run:
                            print(f"      Would recover: '{title[:40]}...' -> {validated}")
                    else:
                        total_still_unknown += 1
                else:
                    total_still_unknown += 1

                # Check limit again
                if limit and processed_count >= limit:
                    break

            if recovered_in_church > 0:
                print(f"   📂 {church_name}: {recovered_in_church} speakers recovered (of {unknown_in_church} unknown)")

                # Write updated CSV
                if not dry_run and modified:
                    fieldnames = ["date", "status", "speaker", "title", "url", "last_checked", "language", "type", "description", "duration", "church", "first_scraped", "video_status", "video_removed_date"]
                    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                        writer.writeheader()
                        writer.writerows(rows)

            # Check limit
            if limit and processed_count >= limit:
                print(f"\n   ⚠️ Limit of {limit} entries reached.")
                break

        except Exception as e:
            print(f"   ❌ Error processing {csv_file}: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "="*60)
    print("📊 SPEAKER RECOVERY SUMMARY")
    print(f"   📋 Total Unknown Speaker entries: {total_unknown}")
    print(f"   ✅ Speakers recovered: {total_recovered} ({100*total_recovered/max(total_unknown,1):.1f}%)")
    print(f"   ❓ Still unknown: {total_still_unknown}")

    if recovered_speakers:
        print("\n   🎤 Top recovered speakers:")
        for speaker, count in sorted(recovered_speakers.items(), key=lambda x: -x[1])[:20]:
            print(f"      {speaker}: {count}")

    print("="*60)

    # Regenerate master CSV if changes were made
    if total_recovered > 0 and not dry_run:
        print("\n📊 Regenerating master CSV...")
        generate_master_csv()

    return total_recovered, total_still_unknown


def fix_corrupt_entries(data_dir, dry_run=False, churches=None):
    """
    Fix corrupt data entries in CSV files.

    Fixes:
    1. Speakers that are dates/numbers (e.g., "251019", "032722") → "Unknown Speaker"
    2. Speakers that are category titles (e.g., "Worship", "Testimony") → "Unknown Speaker"
    3. Invalid dates ("0000-00-00", "Error") → "Unknown Date"
    4. Cloverdale Bibleway date fix: 2-digit years (63-xxxx → 1963-xxxx)
    5. Shekinah Tabernacle date fix: YYMMDD misparse
    6. Remove entries with invalid URLs (e.g., "Full Sermon")
    """
    import pandas as pd

    print("="*60)
    print("🔧 FIXING CORRUPT ENTRIES")
    print("="*60)
    if dry_run:
        print("   [DRY RUN - No changes will be made]")

    # Category titles that should not be speakers
    CATEGORY_AS_SPEAKER = {
        'church service', 'song', 'worship', 'testimony', 'bible study',
        'sermon', 'message', 'service', 'youth service', 'sunday service'
    }

    # Load invalid speakers from config
    invalid_speakers_config = set()
    try:
        with open(SPEAKERS_CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
            invalid_speakers_config = set(config.get('invalid_speakers', []))
    except Exception as e:
        print(f"   ⚠️ Could not load speakers config: {e}")

    total_fixes = {
        'speaker_dates': 0,
        'speaker_categories': 0,
        'speaker_invalid_config': 0,
        'invalid_dates': 0,
        'cloverdale_dates': 0,
        'shekinah_dates': 0,
        'invalid_urls': 0
    }

    churches_to_process = []
    for item in sorted(os.listdir(data_dir)):
        item_path = os.path.join(data_dir, item)
        if os.path.isdir(item_path) and item not in ['__pycache__', '.git']:
            if churches:
                if item.lower().replace('_', ' ') in churches.lower() or churches.lower() in item.lower():
                    churches_to_process.append(item)
            else:
                churches_to_process.append(item)

    for church_folder in churches_to_process:
        church_display = church_folder.replace('_', ' ')

        # Find CSV file - it's in data_dir with format {church_folder}_Summary.csv
        csv_filename = f"{church_folder}_Summary.csv"
        csv_path = os.path.join(data_dir, csv_filename)
        if not os.path.exists(csv_path):
            continue

        try:
            df = pd.read_csv(csv_path)
        except Exception as e:
            print(f"   ❌ Error reading {church_display}: {e}")
            continue

        church_fixes = {k: 0 for k in total_fixes.keys()}
        modified = False
        rows_to_drop = []

        for idx, row in df.iterrows():
            speaker = str(row.get('speaker', ''))
            date = str(row.get('date', ''))
            url = str(row.get('url', ''))

            # Fix 1: Speakers that are dates/numbers
            if speaker and speaker != 'Unknown Speaker':
                # Check if speaker is mostly digits (like 251019, 032722)
                digits_only = re.sub(r'[^0-9]', '', speaker)
                if len(digits_only) >= 5 and len(digits_only) / len(speaker.replace(' ', '')) > 0.7:
                    if not dry_run:
                        df.at[idx, 'speaker'] = 'Unknown Speaker'
                    church_fixes['speaker_dates'] += 1
                    modified = True
                    continue
                # Check if speaker is just a small number (part number like 1, 2, 24)
                if re.match(r'^\d{1,3}$', speaker.strip()):
                    if not dry_run:
                        df.at[idx, 'speaker'] = 'Unknown Speaker'
                    church_fixes['speaker_dates'] += 1
                    modified = True
                    continue

            # Fix 2: Speakers that are category titles
            if speaker and speaker.lower().strip() in CATEGORY_AS_SPEAKER:
                if not dry_run:
                    df.at[idx, 'speaker'] = 'Unknown Speaker'
                church_fixes['speaker_categories'] += 1
                modified = True
                continue

            # Fix 2b: Speakers in invalid_speakers config (program names, etc.)
            if speaker and speaker in invalid_speakers_config:
                if not dry_run:
                    df.at[idx, 'speaker'] = 'Unknown Speaker'
                church_fixes['speaker_invalid_config'] += 1
                modified = True
                continue

            # Fix 3: Invalid dates
            if date in ['0000-00-00', 'Error', '']:
                if not dry_run:
                    df.at[idx, 'date'] = 'Unknown Date'
                church_fixes['invalid_dates'] += 1
                modified = True

            # Fix 3b: Impossible future dates (> 2100 or years like 3023, etc.)
            date_match = re.match(r'^(\d{4})-\d{2}-\d{2}$', date)
            if date_match:
                year = int(date_match.group(1))
                if year > 2100 or year < 1900:
                    if not dry_run:
                        df.at[idx, 'date'] = 'Unknown Date'
                    church_fixes['invalid_dates'] += 1
                    modified = True

            # Fix 4: Cloverdale Bibleway - fix 2-digit year dates (2063 → 1963)
            if church_folder == 'Cloverdale_Bibleway':
                if re.match(r'^20[5-9]\d-', date):  # Years 2050-2099 likely wrong
                    fixed_year = int(date[:4]) - 100  # 2063 → 1963
                    fixed_date = f"{fixed_year}{date[4:]}"
                    if not dry_run:
                        df.at[idx, 'date'] = fixed_date
                    church_fixes['cloverdale_dates'] += 1
                    modified = True

            # Fix 5: Shekinah Tabernacle - fix YYMMDD misparse
            # Pattern: 2027-07-25 should be 2025-07-27 (date and year swapped)
            if church_folder == 'Shekinah_Tabernacle':
                if re.match(r'^20(2[7-9]|[3-9]\d)-', date):  # Years 2027-2099
                    # These are likely DDMMYY being parsed as YYMMDD
                    # 270725 was parsed as 2027-07-25, but should be 2025-07-27
                    # Check if day > 12 (month can't be > 12)
                    parts = date.split('-')
                    if len(parts) == 3:
                        year, month, day = parts
                        if int(day) > 12 or int(year) > 2026:
                            # Swap day and year suffix: 2027-07-25 → 2025-07-27
                            new_year = f"20{day}"
                            new_day = year[2:]  # Get last 2 digits of misread year
                            fixed_date = f"{new_year}-{month}-{new_day}"
                            if not dry_run:
                                df.at[idx, 'date'] = fixed_date
                            church_fixes['shekinah_dates'] += 1
                            modified = True

            # Fix 6: Invalid URLs
            if url and url.lower() in ['full sermon', 'unknown', '']:
                rows_to_drop.append(idx)
                church_fixes['invalid_urls'] += 1

        # Remove rows with invalid URLs
        if rows_to_drop and not dry_run:
            df = df.drop(rows_to_drop)
            modified = True

        # Save if modified
        if modified and not dry_run:
            df.to_csv(csv_path, index=False)

        # Report church fixes
        church_total = sum(church_fixes.values())
        if church_total > 0:
            for k, v in church_fixes.items():
                total_fixes[k] += v
            print(f"   📂 {church_display}: {church_total} fixes")
            for k, v in church_fixes.items():
                if v > 0:
                    print(f"      - {k.replace('_', ' ')}: {v}")

    # Summary
    print("\n" + "="*60)
    print("📊 CORRUPT ENTRY FIX SUMMARY")
    grand_total = sum(total_fixes.values())
    print(f"   Total fixes: {grand_total}")
    for k, v in total_fixes.items():
        if v > 0:
            print(f"   - {k.replace('_', ' ')}: {v}")
    print("="*60)

    if grand_total > 0 and not dry_run:
        print("\n📊 Regenerating master CSV...")
        generate_master_csv()

    return total_fixes


def add_preservation_metadata(data_dir, dry_run=False):
    """
    Add preservation metadata fields to all CSV files.

    New fields:
    - first_scraped: When entry was first added (defaults to last_checked if not set)
    - video_status: Current availability ("available", "unavailable", "unknown")
    - video_removed_date: When video was detected as removed (if applicable)
    """
    import pandas as pd
    from datetime import datetime

    print("="*60)
    print("📦 ADDING PRESERVATION METADATA")
    print("="*60)
    if dry_run:
        print("   [DRY RUN - No changes will be made]")

    today = datetime.now().strftime('%Y-%m-%d')
    total_updated = 0

    # Find all summary CSVs
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('_Summary.csv')]

    for csv_file in sorted(csv_files):
        csv_path = os.path.join(data_dir, csv_file)
        church_name = csv_file.replace('_Summary.csv', '').replace('_', ' ')

        try:
            df = pd.read_csv(csv_path)
        except Exception as e:
            print(f"   ❌ Error reading {church_name}: {e}")
            continue

        modified = False
        added_fields = []

        # Add first_scraped column (defaults to last_checked or today)
        if 'first_scraped' not in df.columns:
            df['first_scraped'] = df.get('last_checked', today)
            added_fields.append('first_scraped')
            modified = True

        # Add video_status column (defaults to 'available' for Success entries)
        if 'video_status' not in df.columns:
            df['video_status'] = df['status'].apply(
                lambda s: 'available' if s in ['Success', 'Metadata Only', 'No Transcript'] else 'unknown'
            )
            added_fields.append('video_status')
            modified = True

        # Add video_removed_date column (empty for now)
        if 'video_removed_date' not in df.columns:
            df['video_removed_date'] = ''
            added_fields.append('video_removed_date')
            modified = True

        if modified:
            total_updated += 1
            if added_fields:
                print(f"   📂 {church_name}: Added {', '.join(added_fields)}")
            if not dry_run:
                df.to_csv(csv_path, index=False)

    print("\n" + "="*60)
    print(f"📊 PRESERVATION METADATA SUMMARY")
    print(f"   Files updated: {total_updated}")
    print("="*60)

    if total_updated > 0 and not dry_run:
        print("\n📊 Regenerating master CSV...")
        generate_master_csv()

    return total_updated


def enrich_metadata(data_dir, dry_run=False, churches=None, limit=None):
    """
    Fetch missing metadata from YouTube for entries that have:
    - Missing duration (0 or null)
    - Missing description (empty)
    - Unknown Date

    Uses pytubefix to fetch video info and fills in missing fields.
    """
    print("="*60)
    print("🔄 ENRICHING METADATA FROM YOUTUBE")
    print("="*60)
    if dry_run:
        print("   [DRY RUN - No changes will be made]")
    if churches:
        print(f"   Churches filter: {churches}")
    if limit:
        print(f"   Limit: {limit} entries")

    # Parse churches filter
    church_filter = None
    if churches:
        church_filter = [c.strip().replace(' ', '_') for c in churches.split(',')]

    total_enriched = 0
    total_failed = 0
    processed = 0

    # Find all Summary CSV files
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('_Summary.csv')]

    for csv_file in sorted(csv_files):
        if limit and processed >= limit:
            break

        church_name = csv_file.rsplit('_Summary.csv', 1)[0]

        # Apply church filter
        if church_filter and church_name not in church_filter:
            continue

        csv_path = os.path.join(data_dir, csv_file)

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames
                rows = list(reader)
        except Exception as e:
            print(f"   ⚠️ Error reading {csv_file}: {e}")
            continue

        # Find entries needing enrichment (missing duration, description, or Unknown Date)
        entries_to_enrich = []
        for i, row in enumerate(rows):
            url = row.get('url', '').strip()
            if not url or 'youtube.com' not in url and 'youtu.be' not in url:
                continue

            needs_enrichment = False
            missing = []

            # Check duration
            duration = row.get('duration', '')
            if not duration or duration in ['', '0', '0.0'] or float(duration or 0) == 0:
                needs_enrichment = True
                missing.append('duration')

            # Check description
            description = row.get('description', '')
            if not description or description.strip() == '':
                needs_enrichment = True
                missing.append('description')

            # Check date
            date = row.get('date', '')
            if date in ['Unknown Date', 'Unknown', 'Error', '']:
                needs_enrichment = True
                missing.append('date')

            if needs_enrichment:
                entries_to_enrich.append((i, row, missing))

        if not entries_to_enrich:
            continue

        print(f"\n   📂 {church_name.replace('_', ' ')}: {len(entries_to_enrich)} entries to enrich")

        modified = False
        for idx, (row_idx, row, missing) in enumerate(entries_to_enrich):
            if limit and processed >= limit:
                break
            processed += 1

            title = row.get('title', 'Unknown')[:50]
            url = row.get('url', '')

            print(f"      [{idx+1}/{len(entries_to_enrich)}] {title}... (missing: {', '.join(missing)})")

            if dry_run:
                continue

            try:
                time.sleep(1)  # Rate limiting
                yt_obj = youtube_with_timeout(url, use_oauth=False, allow_oauth_cache=True)
                if yt_obj is None:
                    total_failed += 1
                    continue

                enriched_fields = []

                # Enrich duration
                if 'duration' in missing and yt_obj.length:
                    duration_min = round(yt_obj.length / 60, 1)
                    rows[row_idx]['duration'] = str(duration_min)
                    enriched_fields.append(f'duration={duration_min}m')
                    modified = True

                # Enrich description
                if 'description' in missing and yt_obj.description:
                    rows[row_idx]['description'] = yt_obj.description[:500]
                    enriched_fields.append('description')
                    modified = True

                # Enrich date
                if 'date' in missing:
                    new_date = determine_sermon_date(title, yt_obj.description or '', yt_obj)
                    if new_date and new_date != "Unknown Date":
                        rows[row_idx]['date'] = new_date
                        enriched_fields.append(f'date={new_date}')
                        modified = True
                    elif yt_obj.publish_date:
                        new_date = yt_obj.publish_date.strftime("%Y-%m-%d")
                        rows[row_idx]['date'] = new_date
                        enriched_fields.append(f'date={new_date}')
                        modified = True

                if enriched_fields:
                    total_enriched += 1
                    print(f"         ✅ Enriched: {', '.join(enriched_fields)}")
                else:
                    total_failed += 1
                    print(f"         ⚠️ No data available")

            except Exception as e:
                total_failed += 1
                print(f"         ❌ Error: {str(e)[:60]}")

        # Write back the CSV if modified
        if modified and not dry_run:
            try:
                with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    writer.writerows(rows)
                print(f"      💾 Saved {csv_file}")
            except Exception as e:
                print(f"      ❌ Error saving {csv_file}: {e}")

    print("\n" + "="*60)
    print("📊 ENRICHMENT SUMMARY")
    print(f"   Processed: {processed}")
    print(f"   Enriched: {total_enriched}")
    print(f"   Failed: {total_failed}")
    print("="*60)

    if total_enriched > 0 and not dry_run:
        print("\n📊 Regenerating master CSV...")
        generate_master_csv()

    return total_enriched, total_failed


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
    # Removed recursive NAME_PATTERN repetition since NAME_PATTERN handles multi-word names
    pattern = rf'{HONORIFIC_PATTERN}[\s\.]+({NAME_PATTERN})'
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
    """Pattern 2: Multi-Speaker 'Bro. Name & Bro. Name'"""
    # Updated to rely on NAME_PATTERN
    pattern = rf'(?:Bro\.|Brother|Sis\.|Sister|Pastor)\s+({NAME_PATTERN})\s*&\s*(?:Bro\.|Brother|Sis\.|Sister|Pastor)\s+({NAME_PATTERN})'
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        name1 = clean_name(match.group(1))
        name2 = clean_name(match.group(2))
        if name1 and name2:
            return f"{name1}, {name2}"
    return None
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
        raw_name = match.group(1)
        raw_name = raw_name.replace(" and ", ", ")
        name = clean_name(raw_name)
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
    
    # Delegate core validity check to the robust function
    if not is_valid_person_name(speaker, title):
        return "Unknown Speaker"

    speaker = speaker.strip()
    
    # Handle "By Name" pattern FIRST - extract just the name
    by_match = re.match(r'^By\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)$', speaker)
    if by_match:
        extracted = by_match.group(1)
        if re.match(r'^[A-Z][a-z]+\s+[A-Z][a-z]+', extracted):
            speaker = extracted
        else:
            return "Unknown Speaker"
    
    # Block names starting with prepositions/articles/conjunctions
    if re.match(r'^(?:in|to|of|by|on|at|for|and|the|a|an)\s+', speaker, re.IGNORECASE):
        return "Unknown Speaker"
    
    # Block names containing " and " unless it looks like a multi-speaker pair with honorifics
    # e.g. "Bro Ben and Bro John" is OK. "Faith and Hope" is suspicious.
    if " and " in speaker.lower() or " & " in speaker:
        # Check if honorific present
        if not re.search(r'(?:Bro|Sis|Pas|Rev|Bish|Eld)', speaker, re.IGNORECASE):
             # Exception: "Name Surname & Name Surname" (2+ words each side)
             parts = re.split(r'\s+(?:and|&)\s+', speaker)
             if len(parts) == 2 and len(parts[0].split()) >= 2 and len(parts[1].split()) >= 2:
                 pass # Allow "John Doe & Jane Doe"
             else:
                 return "Unknown Speaker"

    # Block names ending with "and" or "&" (incomplete multi-speaker) or stop words
    if re.search(r'\s+(?:and|&|in|on|at|to|by|for)\s*$', speaker, re.IGNORECASE):
        return "Unknown Speaker"
    
    # CRITICAL: Reject Date-like speakers (Month names) inside final_validation too
    if re.search(r'\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\b', speaker, re.IGNORECASE):
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

    # --- ROUND 9 AUDIT ADDITIONS ---

    # Block speakers starting with articles (The, A, An) - these are sermon titles
    if re.match(r'^(The|A|An)\s+[A-Z]', speaker):
        return "Unknown Speaker"

    # Block speakers containing Part/Pt numbers - these are sermon series
    if re.search(r'\b(Pt\.?|Part)\s*\d', speaker, re.IGNORECASE):
        return "Unknown Speaker"

    # Block speakers containing 4+ digit numbers (dates, sermon codes)
    if re.search(r'\d{4,}', speaker):
        return "Unknown Speaker"

    # Block speakers with parentheses (usually sermon details)
    if '(' in speaker or ')' in speaker:
        return "Unknown Speaker"

    # Block gerund + preposition/article patterns (sermon title patterns)
    if re.match(r'^[A-Z][a-z]+ing\s+(The|In|To|Of|With|On|A|An|For|At|By)\s+', speaker, re.IGNORECASE):
        return "Unknown Speaker"

    # Block "X Of Y" patterns that are clearly sermon titles (not names like "Son of Man")
    if re.search(r'\s+Of\s+[A-Z][a-z]+', speaker) and not re.match(r'^[A-Z][a-z]+\s+[A-Z][a-z]+\s+Of\s+', speaker):
        # Exception: allow "X of Y" only if first two words look like a name
        words = speaker.split()
        if len(words) < 2 or words[0].lower() in INVALID_NAME_TERMS:
            return "Unknown Speaker"

    # Block single-word speakers that match INVALID_NAME_TERMS
    if re.match(r'^[A-Z][a-z]+$', speaker):
        if speaker.lower() in INVALID_NAME_TERMS:
            return "Unknown Speaker"

    # Block speakers that match CATEGORY_TITLES exactly
    if speaker in CATEGORY_TITLES or speaker.title() in CATEGORY_TITLES:
        return "Unknown Speaker"

    # Block speakers starting with numbers
    if re.match(r'^\d', speaker):
        return "Unknown Speaker"

    # Block incomplete/fragment names
    incomplete_names = {'de la', 'William Marrion', 'H Simmons', 'Apostle Bernie'}
    if speaker in incomplete_names:
        return "Unknown Speaker"

    # Block "Name Name The Title" patterns (sermon title attached to speaker name)
    if re.search(r'^[A-Z][a-z]+\s+[A-Z][a-z]+\s+The\s+[A-Z]', speaker):
        return "Unknown Speaker"

    # Block "Name Name + Title Words" patterns (e.g., "Chad Lamb Religion Versus Relationship")
    title_indicators = ['Religion', 'Versus', 'Relationship', 'Discerning', 'Unexpected', 'Fulfillment',
                        'Seven', 'Seals', 'True', 'Pride', 'Token', 'Tape', 'Visions', 'Tactics',
                        'Power', 'Gene', 'Arrow', 'Attitude', 'Importance', 'Promise', 'Hands',
                        'Severity', 'Understanding', 'Deceived']
    for indicator in title_indicators:
        if re.search(r'^[A-Z][a-z]+\s+[A-Z][a-z]+\s+' + indicator + r'\b', speaker, re.I):
            return "Unknown Speaker"

    # Block patterns ending with "Of Something" that look like sermon titles
    if re.search(r"'s\s+[A-Z][a-z]+\s+Of\s+", speaker):  # "God's Arrow Of"
        return "Unknown Speaker"
    if re.search(r'^[A-Z][a-z]+\s+Of\s+[A-Z][a-z]+$', speaker):  # "Attitude Of Love"
        return "Unknown Speaker"
    if re.search(r'Of\s+[A-Z][a-z]+$', speaker) and len(speaker.split()) > 3:  # Long "X Of Y" patterns
        return "Unknown Speaker"

    # --- END ROUND 9 AUDIT ADDITIONS ---

    # Apply normalization
    speaker_norm = normalize_speaker(speaker, title)
    speaker = speaker_norm

    if not speaker:
        return "Unknown Speaker"
    
    # Clean the name
    speaker_clean = clean_name(speaker)
    speaker = speaker_clean

    if not speaker:
        return "Unknown Speaker"
    
    # Validate
    if not is_valid_name(speaker):
        # Try one more time with just the first two words
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

# --- WMB DATA LOADING ---
WMB_TRANSCRIPTS_MAP = {}
WMB_CODES_SET = set()

def load_wmb_transcripts_map(wmb_path="data/William_Branham_Sermons"):
    """
    Load William Branham sermon catalogue for cross-referencing.
    Populates WMB_TRANSCRIPTS_MAP (Date -> List of (Code, Title))
    and WMB_CODES_SET (Set of Codes e.g. '60-1211E').
    """
    global WMB_TRANSCRIPTS_MAP, WMB_CODES_SET
    WMB_TRANSCRIPTS_MAP = {}
    WMB_CODES_SET = set()
    
    if not os.path.exists(wmb_path):
        return

    for filename in os.listdir(wmb_path):
        if not filename.endswith(".txt"):
            continue
            
        # Format: YY-MMDD[Suf] - Title.txt
        # Example: 60-1211E - The Laodicean Church Age.txt
        parts = filename.split(" - ", 1)
        if len(parts) != 2:
            continue
            
        code_part = parts[0].strip() # 60-1211E
        title_part = parts[1].replace(".txt", "").strip() # The Laodicean Church Age
        
        # Parse date from Code
        # Code: YY-MMDD[Suf]
        # Regex: (\d{2})-(\d{2})(\d{2})([A-Za-z]?)
        match = re.match(r'^(\d{2})-(\d{2})(\d{2})([A-Za-z]?)$', code_part)
        if match:
            yy, mm, dd, suf = match.groups()
            year = "19" + yy
            date_str = f"{year}-{mm}-{dd}"
            
            if date_str not in WMB_TRANSCRIPTS_MAP:
                WMB_TRANSCRIPTS_MAP[date_str] = []
            
            WMB_TRANSCRIPTS_MAP[date_str].append((code_part, title_part))
            WMB_CODES_SET.add(code_part)

def identify_speaker_dynamic(title, description, known_speakers, date_str=None):
    """
    Enhanced speaker identification using 70+ pattern extraction functions.
    Supports cross-referencing valid sermon dates with William Branham database.
    """
    # Initialize WMB map if empty
    if not WMB_TRANSCRIPTS_MAP:
        load_wmb_transcripts_map()
    
    # CRITICAL: WMB Cross-Check (Dates <= 1965)
    if date_str and date_str != "Unknown Date" and WMB_TRANSCRIPTS_MAP:
        try:
             # Check if date is pre-1966
             ds_parts = date_str.split('-')
             if len(ds_parts) == 3 and int(ds_parts[0]) <= 1965:
                 wmb_candidates = WMB_TRANSCRIPTS_MAP.get(date_str, [])
                 title_lower = title.lower()
                 
                 found_wmb = False
                 for code, wmb_title in wmb_candidates:
                     wmb_title_lower = wmb_title.lower()
                     code_lower = code.lower()
                     
                     # Check 1: DateCode match (e.g. "60-1211E") anywhere in title
                     if code_lower in title_lower:
                         found_wmb = True
                         break
                         
                     # Check 2: Strong Title Match
                     if len(wmb_title) > 8 and wmb_title_lower in title_lower:
                         found_wmb = True
                         break
                         
                     # Check 3: Candidate title matches WMB title
                     if title_lower in wmb_title_lower and len(title) > 8:
                         found_wmb = True
                         break
                 
                 # Check 4: DateCode prefix checking (e.g. "60-1211" without suffix)
                 if not found_wmb:
                     short_code = f"{ds_parts[0][2:]}-{ds_parts[1]}{ds_parts[2]}" # YY-MMDD
                     if short_code in title:
                         found_wmb = True
                         
                 if found_wmb:
                     return "William M. Branham", True

        except Exception:
            pass

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
    search_text_casefold = search_text.casefold()
    for name in known_speakers:
        if name and name.casefold() in search_text_casefold:
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
        # --- NEW VALIDATION AND NORMALIZATION ---
        # Apply smart correction first
        title_speaker = smart_speaker_correction(title_speaker, title)
        
        # Check against blocklists
        if title_speaker in UNWANTED_SPEAKERS or title_speaker in CATEGORY_TITLES or title_speaker in SONG_TITLES:
             title_speaker = None
        # ----------------------------------------
        
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
        # --- NEW VALIDATION AND NORMALIZATION ---
        desc_speaker = smart_speaker_correction(desc_speaker, title)
        
        # Apply strict validity check
        if not is_valid_person_name(desc_speaker, title):
             pass # Invalid name
        elif desc_speaker in UNWANTED_SPEAKERS or desc_speaker in CATEGORY_TITLES or desc_speaker in SONG_TITLES:
             pass # Continue to other fallbacks
        else:
            return normalize_speaker(desc_speaker, title), False
        # ----------------------------------------
    
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
    if any(term in text_to_search for term in ["youth camp", "youth service", "youth meeting", "youth program", "youth panel", "youth skit", "youth retreat", "youth bible", "youth concert"]):
        return "Youth Service"
    if "sunday school" in title_lower:
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
    
    # Convention/Camp Meeting/Conference
    if any(term in text_to_search for term in ["convention", "conference", "camp meeting", "campmeeting"]):
        return "Convention"
    
    # Prayer Meeting/Bible Study
    if any(term in text_to_search for term in ["prayer meeting", "prayer service"]):
        return "Prayer Meeting"
    if any(term in text_to_search for term in ["bible study", "bible class"]):
        # Collapse Bible Study into the generic Service category.
        return "Church Service"
    
    # Q&A Sessions
    if any(term in text_to_search for term in ["q&a", "q & a", "questions and answers", "question and answer"]):
        return "Q&A Session"
    
    # Testimony Service (English and Spanish)
    if any(term in text_to_search for term in ["testimony", "testimonies", "testimonio", "testimonios", "testimonial"]):
        return "Testimonies"
    
    # Sermon Clip
    if any(term in text_to_search for term in ["clip", "excerpt", "highlight"]): 
        return "Sermon Clip"
    
    # Song Special - explicit title/description match first
    if any(term in text_to_search for term in ["song special", "special song"]):
        return "Song Special"

    # Song / Worship - explicit song service or medley patterns
    if any(term in text_to_search for term in ["song service", "praise medley", "worship medley", "singing service", "praise service"]):
        return "Song / Worship"

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
    
    # Church Service - videos 60+ minutes with identified speaker
    if duration_minutes >= 60:
        return "Church Service"
    
    # Short Clip - videos under 60 minutes (not matching other categories)
    if duration_minutes > 0 and duration_minutes < 60:
        return "Short Clip"
    
    # Default when duration unknown
    return "Church Service"


def apply_manual_metadata_overrides(title, speaker, video_type):
    """Central place for one-off corrections that must persist via update_sermons.py.

    Returns (speaker, video_type).
    """
    title_text = (title or "").strip()
    speaker_text = (speaker or "").strip()
    type_text = (video_type or "").strip()

    # 1) "I Am Joseph" is a sermon title, not a speaker.
    if re.search(r"\bi\s*am\s*joseph\b", title_text, re.IGNORECASE):
        speaker_text = "Unknown Speaker"

    # 2) "Br Ed Byskal ... witness regarding ... William Branham" should be Ed Byskal.
    if (
        re.search(r"\bed\s+byskal\b", title_text, re.IGNORECASE)
        and re.search(r"\b(testimony|witness)\b", title_text, re.IGNORECASE)
        and re.search(r"\bbranham\b", title_text, re.IGNORECASE)
        and speaker_text in {"", "Unknown Speaker", "William M. Branham"}
    ):
        speaker_text = "Ed Byskal"

    # 3) Remove Bible Study as a speaker; fold Bible Study into Service.
    if speaker_text.casefold() in {"bible study", "prayer bible study"}:
        speaker_text = "Unknown Speaker"
        type_text = "Church Service"

    if type_text.casefold() in {"bible study", "prayer bible study"}:
        type_text = "Church Service"

    return speaker_text, type_text

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


def extract_guss_archive_date(text):
    """
    Extract date from Christ Witness Guss archive format: YY MMDD or vYY MMDD
    Examples: 
      - "89 0115 Ed Byskal" -> 1989-01-15
      - "v89 0115M Ed Byskal" -> 1989-01-15 (M suffix for morning)
      - "08 0511 Bro Ed Byskal" -> 2008-05-11
    
    Returns: date string in YYYY-MM-DD format, or None if not matched
    """
    # Match optional 'v' prefix, then YY followed by space, then MMDD with optional M suffix
    match = re.match(r'^v?(\d{2})\s+(\d{2})(\d{2})M?\s', text.strip(), re.IGNORECASE)
    if not match:
        return None
    
    year_2digit = int(match.group(1))
    month = match.group(2)
    day = match.group(3)
    
    # Convert 2-digit year:
    # 80-99 = 1980-1999 (most Cloverdale archives are from 80s/90s)
    # 00-29 = 2000-2029
    if year_2digit >= 80:
        year_4digit = 1900 + year_2digit
    else:
        year_4digit = 2000 + year_2digit
    
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
    
    # Try Christ Witness Guss archive format: YY MMDD or vYY MMDD
    guss_date = extract_guss_archive_date(text)
    if guss_date: return guss_date
    
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
    yt = youtube_with_timeout(url, client=client_type, use_oauth=False, allow_oauth_cache=False)
    if yt is None:
        return None, None
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
    
    # Fetch videos from additional channels (with filtering)
    additional_channels = config.get('additional_channels', [])
    for add_channel in additional_channels:
        add_channel_name = add_channel.get('name', 'Additional Channel')
        print(f"   📺 Processing additional channel: {add_channel_name}...")
        matching_videos, total_scanned, filtered_out = fetch_additional_channel_videos(add_channel, limit=None)
        if matching_videos:
            all_videos.extend(matching_videos)
    
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
                    yt_obj = youtube_with_timeout(video_url, use_oauth=False, allow_oauth_cache=True)
                    if yt_obj is None:
                        continue
                    description = yt_obj.description or ""

                    # Update metadata if we got better info
                    if description and not existing.get('description'):
                        existing['description'] = description.replace('\n', ' ').replace('\r', ' ')[:500]
                    if existing.get('date') == 'Unknown Date':
                        existing['date'] = determine_sermon_date(title, description, yt_obj)
                    if existing.get('speaker') == 'Unknown Speaker':
                        speaker, _ = identify_speaker_dynamic(title, description, known_speakers, date_str=existing.get('date'))
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
            yt_obj = youtube_with_timeout(video_url, use_oauth=False, allow_oauth_cache=True)
            if yt_obj is None:
                # Add with minimal info on timeout
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
                    "church": church_name,
                    "first_scraped": today_str,
                    "video_status": "unknown",
                    "video_removed_date": ""
                }
                new_count += 1
                continue
            description = yt_obj.description or ""
            sermon_date = determine_sermon_date(title, description, yt_obj)
            
            # Identify speaker from title and description
            speaker, _ = identify_speaker_dynamic(title, description, known_speakers, date_str=sermon_date)
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

            speaker, video_type = apply_manual_metadata_overrides(title, speaker, video_type)
            
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
                "church": church_name,
                "first_scraped": today_str,
                "video_status": "available",
                "video_removed_date": ""
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
                "church": church_name,
                "first_scraped": today_str,
                "video_status": "unknown",
                "video_removed_date": ""
            }
            new_count += 1
    
    # Write merged summary CSV
    csv_path = get_summary_file_path(church_name, ".csv")
    summary_list = list(existing_history.values())
    
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["date", "status", "speaker", "title", "url", "last_checked", "language", "type", "description", "duration", "church", "first_scraped", "video_status", "video_removed_date"])
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

    NOTE: This function is conservative - it only resets CLEARLY invalid speakers
    (like dates, category titles, empty strings). It preserves speakers that were
    set by heal, even if they don't pass strict is_valid_person_name() checks.
    The heal process handles comprehensive speaker validation separately.
    """
    cleaned_history = {}
    fixed_count = 0

    # Patterns that are CLEARLY invalid speakers (not person names)
    clearly_invalid_patterns = [
        r'^\d{4}-\d{2}-\d{2}',  # Date patterns like 2024-01-15
        r'^\d{6,}',  # Long number sequences
        r'^https?://',  # URLs
    ]

    for url, entry in history.items():
        speaker = entry.get('speaker', '').strip()

        # Keep if already Unknown Speaker
        if speaker == 'Unknown Speaker' or not speaker:
            cleaned_history[url] = entry
            continue

        # Keep if it has honorific prefixes (likely valid)
        if any(prefix in speaker for prefix in ["Brother ", "Sister ", "Bro.", "Sis.", "Pastor ", "Rev.", "Elder "]):
            cleaned_history[url] = entry
            continue

        # Only reset if CLEARLY invalid
        is_clearly_invalid = False

        # Check against clearly invalid patterns
        for pattern in clearly_invalid_patterns:
            if re.match(pattern, speaker):
                is_clearly_invalid = True
                break

        # Check if it's a category title (these are definitely not speakers)
        if speaker in CATEGORY_TITLES:
            is_clearly_invalid = True

        if is_clearly_invalid:
            fixed_entry = entry.copy()
            fixed_entry['speaker'] = 'Unknown Speaker'
            cleaned_history[url] = fixed_entry
            fixed_count += 1
        else:
            # Preserve existing speaker - heal already validated it
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

def process_channel(church_name, config, known_speakers, limit=None, recent_only=False, days_back=None, skip_existing=False, retry_no_transcript_only=False):
    """
    Process a YouTube channel for sermon transcripts.
    
    Args:
        church_name: Name of the church/channel
        config: Channel configuration dict
        known_speakers: Set of known speaker names
        limit: Max number of videos to scan (None = no limit)
        recent_only: Legacy flag for 24-hour mode (deprecated, use days_back=1)
        days_back: Number of days to look back (None = full archive, 7 = last week)
        skip_existing: If True, skip videos that already have transcripts (Success status)
        retry_no_transcript_only: If True, only process videos with "No Transcript" status
    
    Returns:
        dict: Statistics about speaker detection for this channel
    """
    # Initialize stats tracking
    channel_stats = {
        'total_processed': 0,
        'speakers_detected': 0,
        'unknown_speakers': 0,
        'new_speakers': set(),
        'csv_files_processed': [get_summary_file_path(church_name, ".csv")]
    }

    known_casefold = build_known_speakers_casefold_map(known_speakers)
    
    channel_url = config['url']
    clean_channel_name = church_name.replace(' ', '_')
    channel_dir = os.path.join(DATA_DIR, clean_channel_name)
    os.makedirs(channel_dir, exist_ok=True)

    print(f"\\n--------------------------------------------------")
    print(f"Processing Channel: {church_name}")
    if retry_no_transcript_only:
        print(f"   📊 Mode: RETRY NO TRANSCRIPT (only videos without transcripts)")
    elif skip_existing:
        print(f"   📊 Mode: NEW VIDEOS ONLY (skipping existing transcripts)")
    elif limit:
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

    # For retry_no_transcript_only mode, filter history to only "No Transcript" entries
    if retry_no_transcript_only:
        no_transcript_urls = {url for url, entry in history.items() if entry.get('status') == 'No Transcript'}
        no_transcript_count = len(no_transcript_urls)
        print(f"   📋 Found {no_transcript_count} videos with 'No Transcript' status to retry")
        if no_transcript_count == 0:
            print(f"   ✅ No videos to retry - all have transcripts!")
            return channel_stats

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
        
        # Fetch videos from configured playlists (legacy/archived content)
        # Skip playlists when days_back is set - they're for archive content
        playlists = config.get('playlists', [])
        if days_back:
            if playlists:
                print(f"   📋 Skipping {len(playlists)} playlist(s) - using {days_back}-day filter")
        else:
            for playlist_info in playlists:
                playlist_id = playlist_info.get('id')
                playlist_name = playlist_info.get('name', playlist_id)
                if playlist_id:
                    print(f"   📋 Scanning playlist: {playlist_name}...")
                    playlist_videos = fetch_playlist_videos(playlist_id, limit=limit)
                    print(f"      Found {len(playlist_videos)} videos in playlist.")
                    all_videos.extend(playlist_videos)

        # Fetch videos from additional channels (with filtering)
        additional_channels = config.get('additional_channels', [])
        for add_channel in additional_channels:
            add_channel_name = add_channel.get('name', 'Additional Channel')
            print(f"   📺 Processing additional channel: {add_channel_name}...")
            matching_videos, total_scanned, filtered_out = fetch_additional_channel_videos(add_channel, limit=limit, days_back=days_back)
            if matching_videos:
                all_videos.extend(matching_videos)

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

    unique_videos = list(unique_videos_map.values())
    
    # Filter videos based on mode
    if retry_no_transcript_only:
        # Only process videos that had "No Transcript" status
        unique_videos = [v for v in unique_videos 
                        if f"https://www.youtube.com/watch?v={v['videoId']}" in no_transcript_urls]
        print(f"   🔄 Filtered to {len(unique_videos)} videos to retry for transcripts")
    
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
        try: title = video['title']['runs'][0]['text'] or "Unknown Title"
        except: title = "Unknown Title"
        
        # Check if the video is recent enough to be processed (when using days_back filter)
        manual_date = video.get('manual_date')
        if days_back:
            # For videos with manual_date (e.g., Shalom Tabernacle), check date directly
            if manual_date and manual_date != "Unknown Date" and manual_date != "0000-00-00":
                try:
                    video_date = datetime.datetime.strptime(manual_date, "%Y-%m-%d").date()
                    cutoff_date = datetime.date.today() - datetime.timedelta(days=days_back)
                    if video_date < cutoff_date:
                        continue  # Skip if older than days_back
                except:
                    pass  # If date parsing fails, let it through
            else:
                # For regular YouTube videos, use publishedTimeText
                published_time_text = video.get('publishedTimeText', {}).get('simpleText', '')
                if not published_time_text or not parse_published_time(published_time_text, max_days=days_back):
                    continue  # Skip if no date info or older than days_back
        manual_speaker = video.get('manual_speaker')

        if manual_speaker:
            speaker = normalize_speaker(manual_speaker)
            speaker = clean_name(speaker)
            key = speaker_casefold_key(speaker)
            existing = known_casefold.get(key) if key else None
            if existing:
                speaker = existing
            elif speaker and speaker != "Unknown Speaker":
                known_speakers.add(speaker)
                known_casefold[key] = speaker
                channel_stats['new_speakers'].add(speaker)
                save_json_file(SPEAKERS_FILE, known_speakers)
        else:
            # Extract description snippet if available to improve initial detection
            desc_snippet = ""
            try:
                desc_snippet = video.get('descriptionSnippet', {}).get('runs', [{}])[0].get('text', "")
            except: pass
            
            speaker, is_new = identify_speaker_dynamic(title, desc_snippet, known_speakers)
            speaker = normalize_speaker(speaker)
            speaker = clean_name(speaker) 
            if is_new:
                key = speaker_casefold_key(speaker)
                existing = known_casefold.get(key) if key else None
                if existing:
                    speaker = existing
                elif speaker and speaker != "Unknown Speaker" and is_valid_person_name(speaker):
                    print(f"   🎉 LEARNED NEW SPEAKER: {speaker}")
                    known_speakers.add(speaker)
                    known_casefold[key] = speaker
                    channel_stats['new_speakers'].add(speaker)
                    save_json_file(SPEAKERS_FILE, known_speakers)

        video_type = determine_video_type(title, speaker)
        if video_type == "Memorial Service" and speaker != "William M. Branham":
            speaker = "Unknown Speaker"

            speaker, video_type = apply_manual_metadata_overrides(title, speaker, video_type)
        
        # Track speaker detection stats
        channel_stats['total_processed'] += 1
        if speaker and speaker != "Unknown Speaker":
            channel_stats['speakers_detected'] += 1
        else:
            channel_stats['unknown_speakers'] += 1

        history_entry = history.get(video_url)
        needs_download = True
        
        # Skip existing successful transcripts in "new only" mode
        if skip_existing and history_entry and history_entry.get('status') == 'Success':
            history_entry['title'] = title
            history_entry['type'] = video_type
            history_entry['speaker'] = speaker if speaker != "Unknown Speaker" else history_entry.get('speaker', 'Unknown Speaker')
            current_summary_list.append(history_entry)
            continue
        
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
            elif speaker and speaker != "Unknown Speaker" and speaker != old_speaker:
                print(f"[{count}/{total}] 📝 CORRECTING SPEAKER: {old_speaker} -> {speaker}")
                old_key = speaker_casefold_key(old_speaker)
                canonical_old = known_casefold.get(old_key) if old_key else None
                if canonical_old and canonical_old in known_speakers:
                    known_speakers.remove(canonical_old)
                    known_casefold.pop(old_key, None)
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
            # Preserve healed speaker if new detection returns Unknown
            history_entry['speaker'] = speaker if speaker != "Unknown Speaker" else history_entry.get('speaker', 'Unknown Speaker')
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
                speaker, _ = identify_speaker_dynamic(title, description, known_speakers, date_str=sermon_date)
                speaker = normalize_speaker(speaker)
                speaker = clean_name(speaker)
            
            video_type = determine_video_type(title, speaker, transcript_text, yt_obj, description)
            if video_type == "Memorial Service" and speaker != "William M. Branham":
                speaker = "Unknown Speaker"

            speaker, video_type = apply_manual_metadata_overrides(title, speaker, video_type)
            
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

            # Print comprehensive metadata for user verification
            print(f"   📊 SUMMARY DATA:")
            print(f"      Date:     {sermon_date}")
            print(f"      Title:    {title}")
            print(f"      Speaker:  {speaker}")
            print(f"      Type:     {video_type}")
            print(f"      Lang:     {language}")
            print(f"      Status:   {status}")
            print(f"      Duration: {int(duration_minutes) if duration_minutes else 0} min")
            print(f"      URL:      {video_url}")

            current_summary_list.append({
                "date": sermon_date, "status": status, "speaker": speaker,
                "title": title, "url": video_url, "last_checked": today_str,
                "language": language, "type": video_type, "description": desc_for_csv,
                "duration": int(duration_minutes) if duration_minutes else 0,
                "church": church_name,
                "first_scraped": today_str,
                "video_status": "available",
                "video_removed_date": ""
            })
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
            current_summary_list.append({
                "date": "Error", "status": "Failed", "speaker": "Unknown",
                "title": title, "url": video_url, "last_checked": today_str,
                "language": "Unknown", "type": "Unknown", "description": "",
                "duration": 0, "church": church_name,
                "first_scraped": today_str,
                "video_status": "unknown",
                "video_removed_date": ""
            })

    csv_path = get_summary_file_path(church_name, ".csv")
    # --- NEW LOGIC: Ensure every .txt transcript is represented in the summary CSV ---
    channel_dir = os.path.join(DATA_DIR, church_name.replace(' ', '_'))
    txt_files = [f for f in os.listdir(channel_dir) if f.endswith('.txt') and not f.endswith('.timestamped.txt')]
    
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
                "description": "",  # Empty for legacy files
                "duration": 0,
                "church": church_name,
                "first_scraped": last_checked,
                "video_status": "available",
                "video_removed_date": ""
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
    
    # Enable church field population
    final_summary_list = list(url_to_entry.values())
    for entry in final_summary_list:
        entry['church'] = church_name
    
    # Write the updated summary CSV
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["date", "status", "speaker", "title", "url", "last_checked", "language", "type", "description", "duration", "church", "first_scraped", "video_status", "video_removed_date"])
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
                    new_speaker, _ = identify_speaker_dynamic(title, description, known_speakers, date_str=sermon_date)
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
                # Preserve healed speaker if new detection returns Unknown
                entry['speaker'] = speaker if speaker != "Unknown Speaker" else entry.get('speaker', 'Unknown Speaker')
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
                    fieldnames = ['date', 'status', 'speaker', 'title', 'url', 'last_checked', 'language', 'type', 'description', 'duration', 'church', 'first_scraped', 'video_status', 'video_removed_date']
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

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    reset_shutdown_state()  # Clear any previous shutdown state

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
        parser.add_argument('--heal-dates', action='store_true', help="Fix 'Unknown Date' entries by fetching YouTube publish dates.")
        parser.add_argument('--recover-speakers', action='store_true', help="Re-run speaker detection on 'Unknown Speaker' entries to recover speakers.")
        parser.add_argument('--fix-corrupt', action='store_true', help="Fix corrupt data entries (dates as speakers, category names as speakers, invalid dates, etc.)")
        parser.add_argument('--add-preservation-metadata', action='store_true', help="Add preservation metadata fields (first_scraped, video_status, video_removed_date) to CSV files.")
        parser.add_argument('--enrich', action='store_true', help="Fetch missing metadata (duration, description, date) from YouTube for entries missing it.")
        parser.add_argument('--dry-run', action='store_true', help="Show what would be done without making changes (for backfill operations).")
        parser.add_argument('--church', type=str, action='append', help="Specific church(es) to process (can be used multiple times).")
        parser.add_argument('--limit', type=int, default=None, help="Maximum number of files to process.")
        parser.add_argument('--unscraped', action='store_true', help="Only scrape channels that don't have a Summary CSV file yet.")
        parser.add_argument('--list-pending', action='store_true', help="List all videos without transcripts (Metadata Only).")
        parser.add_argument('--retry-metadata', action='store_true', help="Retry fetching transcripts for 'Metadata Only' videos.")
        parser.add_argument('--include-no-transcript', action='store_true', help="Include 'No Transcript' videos when listing or retrying.")
        parser.add_argument('--add-timestamps-video', type=str, metavar='URL', help="Add timestamps to a specific video by URL or video ID.")
        # New scrape mode options
        parser.add_argument('--full', action='store_true', help="Full scrape: Process ALL videos in all channels (re-checks everything).")
        parser.add_argument('--new-only', action='store_true', help="New videos only: Skip videos we already have transcripts for.")
        parser.add_argument('--retry-no-transcript', action='store_true', help="Retry No Transcript: Only retry videos previously marked 'No Transcript'.")
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

        if args.heal_dates:
            print("Healing Unknown Date entries...")
            churches_str = ','.join(args.church) if args.church else None
            heal_unknown_dates(DATA_DIR, dry_run=args.dry_run, churches=churches_str, limit=args.limit)
            return

        if args.recover_speakers:
            print("Recovering speakers from Unknown Speaker entries...")
            churches_str = ','.join(args.church) if args.church else None
            recover_unknown_speakers(DATA_DIR, dry_run=args.dry_run, churches=churches_str, limit=args.limit)
            return

        if args.fix_corrupt:
            print("Fixing corrupt data entries...")
            churches_str = ','.join(args.church) if args.church else None
            fix_corrupt_entries(DATA_DIR, dry_run=args.dry_run, churches=churches_str)
            return

        if args.add_preservation_metadata:
            print("Adding preservation metadata fields...")
            add_preservation_metadata(DATA_DIR, dry_run=args.dry_run)
            return

        if args.enrich:
            print("Enriching metadata from YouTube...")
            churches_str = ','.join(args.church) if args.church else None
            enrich_metadata(DATA_DIR, dry_run=args.dry_run, churches=churches_str, limit=args.limit)
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
            heal_archive(DATA_DIR, force=args.force, churches=args.church)
            return

        channels = load_config()
        if not channels:
            print("No channels found in channels.json.")
            return

        # Startup clean
        raw_speakers = load_json_file(SPEAKERS_FILE)
        known_speakers = deep_clean_speakers_list(raw_speakers)
        save_json_file(SPEAKERS_FILE, known_speakers)

        # Handle --full scrape mode (all videos, all channels)
        if args.full:
            print("\\n" + "="*60)
            print("🔄 FULL SCRAPE: Processing ALL videos in ALL channels")
            print("="*60)
            speakers_before_set = load_json_file(SPEAKERS_FILE)
            all_stats = {'total_processed': 0, 'speakers_detected': 0, 'unknown_speakers': 0, 'by_church': {}, 'new_speakers': set(), 'csv_files_processed': []}
            
            # Filter by specific churches if provided
            channels_to_process = channels
            if args.church:
                channels_to_process = {k: v for k, v in channels.items() if k in args.church}
                print(f"   Filtering to {len(channels_to_process)} specified church(es)")
            
            completed_churches = 0
            total_churches = len(channels_to_process)
            for name, config in channels_to_process.items():
                if should_shutdown():
                    print(f"\n✅ Graceful shutdown: Completed {completed_churches}/{total_churches} churches")
                    print("   Remaining churches will be processed on next run.")
                    break
                channel_stats = process_channel(name, config, known_speakers, limit=args.limit)
                completed_churches += 1
                if channel_stats:
                    all_stats['total_processed'] += channel_stats.get('total_processed', 0)
                    all_stats['speakers_detected'] += channel_stats.get('speakers_detected', 0)
                    all_stats['unknown_speakers'] += channel_stats.get('unknown_speakers', 0)
                    all_stats['new_speakers'].update(channel_stats.get('new_speakers', set()))
                    all_stats['csv_files_processed'].extend(channel_stats.get('csv_files_processed', []))
                    if channel_stats.get('total_processed', 0) > 0:
                        all_stats['by_church'][name] = {
                            'total': channel_stats.get('total_processed', 0),
                            'detected': channel_stats.get('speakers_detected', 0),
                            'unknown': channel_stats.get('unknown_speakers', 0)
                        }
            speakers_after_set = load_json_file(SPEAKERS_FILE)
            all_stats.update(compute_speaker_inventory_delta(speakers_before_set, speakers_after_set))
            all_stats['speakers_changed_to_unknown'] = 0
            write_speaker_detection_log(all_stats, operation_name="Full Scrape (All Videos)")
            if should_shutdown():
                print("\\n⏸️  Full scrape interrupted gracefully.")
            else:
                print("\\n✅ Full scrape complete.")
            return

        # Handle --new-only scrape mode (skip videos we already have)
        if args.new_only:
            print("\\n" + "="*60)
            print("🆕 NEW VIDEOS ONLY: Skipping videos with existing transcripts")
            print("="*60)
            speakers_before_set = load_json_file(SPEAKERS_FILE)
            all_stats = {'total_processed': 0, 'speakers_detected': 0, 'unknown_speakers': 0, 'by_church': {}, 'new_speakers': set(), 'csv_files_processed': []}
            
            channels_to_process = channels
            if args.church:
                channels_to_process = {k: v for k, v in channels.items() if k in args.church}
                print(f"   Filtering to {len(channels_to_process)} specified church(es)")

            completed_churches = 0
            total_churches = len(channels_to_process)
            for name, config in channels_to_process.items():
                if should_shutdown():
                    print(f"\n✅ Graceful shutdown: Completed {completed_churches}/{total_churches} churches")
                    print("   Remaining churches will be processed on next run.")
                    break
                channel_stats = process_channel(name, config, known_speakers, limit=args.limit, skip_existing=True)
                completed_churches += 1
                if channel_stats:
                    all_stats['total_processed'] += channel_stats.get('total_processed', 0)
                    all_stats['speakers_detected'] += channel_stats.get('speakers_detected', 0)
                    all_stats['unknown_speakers'] += channel_stats.get('unknown_speakers', 0)
                    all_stats['new_speakers'].update(channel_stats.get('new_speakers', set()))
                    all_stats['csv_files_processed'].extend(channel_stats.get('csv_files_processed', []))
                    if channel_stats.get('total_processed', 0) > 0:
                        all_stats['by_church'][name] = {
                            'total': channel_stats.get('total_processed', 0),
                            'detected': channel_stats.get('speakers_detected', 0),
                            'unknown': channel_stats.get('unknown_speakers', 0)
                        }
            speakers_after_set = load_json_file(SPEAKERS_FILE)
            all_stats.update(compute_speaker_inventory_delta(speakers_before_set, speakers_after_set))
            all_stats['speakers_changed_to_unknown'] = 0
            write_speaker_detection_log(all_stats, operation_name="New Videos Only Scrape")
            if should_shutdown():
                print("\\n⏸️  New videos scrape interrupted gracefully.")
            else:
                print("\\n✅ New videos scrape complete.")
            return

        # Handle --retry-no-transcript mode (only retry videos marked "No Transcript")
        if args.retry_no_transcript:
            print("\\n" + "="*60)
            if args.days:
                print(f"🔁 RETRY NO TRANSCRIPT: Re-checking videos without transcripts (last {args.days} days)")
            else:
                print("🔁 RETRY NO TRANSCRIPT: Re-checking videos without transcripts (ALL TIME)")
            print("="*60)
            speakers_before_set = load_json_file(SPEAKERS_FILE)
            all_stats = {'total_processed': 0, 'speakers_detected': 0, 'unknown_speakers': 0, 'by_church': {}, 'new_speakers': set(), 'csv_files_processed': []}
            
            channels_to_process = channels
            if args.church:
                channels_to_process = {k: v for k, v in channels.items() if k in args.church}
                print(f"   Filtering to {len(channels_to_process)} specified church(es)")

            completed_churches = 0
            total_churches = len(channels_to_process)
            for name, config in channels_to_process.items():
                if should_shutdown():
                    print(f"\n✅ Graceful shutdown: Completed {completed_churches}/{total_churches} churches")
                    print("   Remaining churches will be processed on next run.")
                    break
                channel_stats = process_channel(name, config, known_speakers, limit=args.limit, days_back=args.days, retry_no_transcript_only=True)
                completed_churches += 1
                if channel_stats:
                    all_stats['total_processed'] += channel_stats.get('total_processed', 0)
                    all_stats['speakers_detected'] += channel_stats.get('speakers_detected', 0)
                    all_stats['unknown_speakers'] += channel_stats.get('unknown_speakers', 0)
                    all_stats['new_speakers'].update(channel_stats.get('new_speakers', set()))
                    all_stats['csv_files_processed'].extend(channel_stats.get('csv_files_processed', []))
                    if channel_stats.get('total_processed', 0) > 0:
                        all_stats['by_church'][name] = {
                            'total': channel_stats.get('total_processed', 0),
                            'detected': channel_stats.get('speakers_detected', 0),
                            'unknown': channel_stats.get('unknown_speakers', 0)
                        }
            speakers_after_set = load_json_file(SPEAKERS_FILE)
            all_stats.update(compute_speaker_inventory_delta(speakers_before_set, speakers_after_set))
            all_stats['speakers_changed_to_unknown'] = 0
            write_speaker_detection_log(all_stats, operation_name="Retry No Transcript")
            if should_shutdown():
                print("\\n⏸️  Retry interrupted gracefully.")
            else:
                print("\\n✅ Retry complete.")
            return

        # When running with --days, process all channels for that time period
        if args.days:
            print(f"\\n🔄 PARTIAL SCRAPE: Last {args.days} days for ALL channels")
            print("="*50)
            speakers_before_set = load_json_file(SPEAKERS_FILE)
            all_stats = {'total_processed': 0, 'speakers_detected': 0, 'unknown_speakers': 0, 'by_church': {}, 'new_speakers': set(), 'csv_files_processed': []}
            completed_churches = 0
            total_churches = len(channels)
            for name, config in channels.items():
                if should_shutdown():
                    print(f"\n✅ Graceful shutdown: Completed {completed_churches}/{total_churches} churches")
                    print("   Remaining churches will be processed on next run.")
                    break
                channel_stats = process_channel(name, config, known_speakers, days_back=args.days)
                completed_churches += 1
                if channel_stats:
                    all_stats['total_processed'] += channel_stats.get('total_processed', 0)
                    all_stats['speakers_detected'] += channel_stats.get('speakers_detected', 0)
                    all_stats['unknown_speakers'] += channel_stats.get('unknown_speakers', 0)
                    all_stats['new_speakers'].update(channel_stats.get('new_speakers', set()))
                    all_stats['csv_files_processed'].extend(channel_stats.get('csv_files_processed', []))
                    if channel_stats.get('total_processed', 0) > 0:
                        all_stats['by_church'][name] = {
                            'total': channel_stats.get('total_processed', 0),
                            'detected': channel_stats.get('speakers_detected', 0),
                            'unknown': channel_stats.get('unknown_speakers', 0)
                        }
            speakers_after_set = load_json_file(SPEAKERS_FILE)
            all_stats.update(compute_speaker_inventory_delta(speakers_before_set, speakers_after_set))
            all_stats['speakers_changed_to_unknown'] = 0
            write_speaker_detection_log(all_stats, operation_name=f"Partial Scrape (Last {args.days} Days)")
            if should_shutdown():
                print(f"\\n⏸️  Partial scrape ({args.days} days) interrupted gracefully.")
            else:
                print(f"\\n✅ Partial scrape ({args.days} days) complete.")
            return

        # When running with --recent, we don't need a menu.
        if args.recent:
            speakers_before_set = load_json_file(SPEAKERS_FILE)
            all_stats = {'total_processed': 0, 'speakers_detected': 0, 'unknown_speakers': 0, 'by_church': {}, 'new_speakers': set(), 'csv_files_processed': []}
            completed_churches = 0
            total_churches = len(channels)
            for name, config in channels.items():
                if should_shutdown():
                    print(f"\n✅ Graceful shutdown: Completed {completed_churches}/{total_churches} churches")
                    print("   Remaining churches will be processed on next run.")
                    break
                channel_stats = process_channel(name, config, known_speakers, days_back=1)
                completed_churches += 1
                if channel_stats:
                    all_stats['total_processed'] += channel_stats.get('total_processed', 0)
                    all_stats['speakers_detected'] += channel_stats.get('speakers_detected', 0)
                    all_stats['unknown_speakers'] += channel_stats.get('unknown_speakers', 0)
                    all_stats['new_speakers'].update(channel_stats.get('new_speakers', set()))
                    all_stats['csv_files_processed'].extend(channel_stats.get('csv_files_processed', []))
                    if channel_stats.get('total_processed', 0) > 0:
                        all_stats['by_church'][name] = {
                            'total': channel_stats.get('total_processed', 0),
                            'detected': channel_stats.get('speakers_detected', 0),
                            'unknown': channel_stats.get('unknown_speakers', 0)
                        }
            speakers_after_set = load_json_file(SPEAKERS_FILE)
            all_stats.update(compute_speaker_inventory_delta(speakers_before_set, speakers_after_set))
            all_stats['speakers_changed_to_unknown'] = 0
            write_speaker_detection_log(all_stats, operation_name="Recent Scrape (Last 24 Hours)")
            if should_shutdown():
                print("\\n⏸️  Recent scrape interrupted gracefully.")
            else:
                print("\\n✅ Recent scrape complete.")
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

            speakers_before_set = load_json_file(SPEAKERS_FILE)
            all_stats = {'total_processed': 0, 'speakers_detected': 0, 'unknown_speakers': 0, 'by_church': {}, 'new_speakers': set(), 'csv_files_processed': []}
            completed_churches = 0
            total_churches = len(unscraped_channels)
            for name, config in unscraped_channels.items():
                if should_shutdown():
                    print(f"\n✅ Graceful shutdown: Completed {completed_churches}/{total_churches} churches")
                    print("   Remaining churches will be processed on next run.")
                    break
                print(f"\\n{'='*50}")
                print(f"SCRAPING: {name}")
                print(f"{'='*50}")
                channel_stats = process_channel(name, config, known_speakers)
                completed_churches += 1
                if channel_stats:
                    all_stats['total_processed'] += channel_stats.get('total_processed', 0)
                    all_stats['speakers_detected'] += channel_stats.get('speakers_detected', 0)
                    all_stats['unknown_speakers'] += channel_stats.get('unknown_speakers', 0)
                    all_stats['new_speakers'].update(channel_stats.get('new_speakers', set()))
                    all_stats['csv_files_processed'].extend(channel_stats.get('csv_files_processed', []))
                    if channel_stats.get('total_processed', 0) > 0:
                        all_stats['by_church'][name] = {
                            'total': channel_stats.get('total_processed', 0),
                            'detected': channel_stats.get('speakers_detected', 0),
                            'unknown': channel_stats.get('unknown_speakers', 0)
                        }
            
            speakers_after_set = load_json_file(SPEAKERS_FILE)
            all_stats.update(compute_speaker_inventory_delta(speakers_before_set, speakers_after_set))
            all_stats['speakers_changed_to_unknown'] = 0
            write_speaker_detection_log(all_stats, operation_name="Unscraped Channels Scrape (CLI)")
            if should_shutdown():
                print("\\n⏸️  Unscraped channels scrape interrupted gracefully.")
            else:
                print(f"\\n✅ Unscraped channels scrape complete.")
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
        print(" 8. Backfill Video Duration (Metadata Scrape)")
        print(" 9. Backfill Missing Timestamped Transcripts")
        print("="*50)
        
        action = input("\n👉 Enter Number: ").strip()
        
        if action == '3':
            print("\n--- DEEP SELF-HEALING & CLEANUP (No Scraping) ---")
            print("This heals existing Summary CSV + transcript headers/filenames.")
            print("Choose one channel or all channels.\n")

            channel_names = list(channels.keys())
            for i, name in enumerate(channel_names, 1):
                print(f"  {i}. {name}")
            print("  0. All Channels")
            choice = input("\n👉 Enter channel number (or 0 for all): ").strip()

            if choice == '0' or not choice:
                heal_archive(DATA_DIR)
                return

            try:
                idx = int(choice) - 1
                if 0 <= idx < len(channel_names):
                    heal_archive(DATA_DIR, churches=[channel_names[idx]])
                else:
                    print("Invalid selection.")
                return
            except ValueError:
                print("Invalid input.")
                return
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
            
            speakers_before_set = load_json_file(SPEAKERS_FILE)
            all_stats = {'total_processed': 0, 'speakers_detected': 0, 'unknown_speakers': 0, 'by_church': {}, 'new_speakers': set(), 'csv_files_processed': []}
            
            if choice == '0':
                print("\n--- PARTIAL SCRAPE SUB-OPTIONS ---")
                print(" 1. Force Scrape All Channels (Standard)")
                print(" 2. Scrape New Channels Only")
                print(" 3. Scrape Channels with Missing Transcripts Only")
                sub_choice = input("\n👉 Enter Mode (1-3): ").strip()

                target_channels = channels.items()
                retry_no_transcript = False

                if sub_choice == '2':
                    filtered = []
                    for name, config in channels.items():
                        path = get_summary_file_path(name, ".csv")
                        if not os.path.exists(path):
                            filtered.append((name, config))
                    target_channels = filtered
                    if not target_channels:
                        print("✅ No new channels found.")
                        return
                    print(f"📊 Found {len(target_channels)} new channels.")

                elif sub_choice == '3':
                    retry_no_transcript = True
                elif sub_choice != '1':
                    print("Invalid selection.")
                    return

                print(f"\n🔄 Scanning channels for videos from the last {days_back} days...\n")
                
                # Sort for consistency (unscraped first)
                def channel_sort_key(item):
                    name, config = item
                    summary_path = get_summary_file_path(name, ".csv")
                    has_summary = os.path.exists(summary_path)
                    return (0 if not has_summary else 1, name.lower())
                
                sorted_channels = sorted(target_channels, key=channel_sort_key)
                
                for name, config in sorted_channels:
                    channel_stats = process_channel(name, config, known_speakers, days_back=days_back, retry_no_transcript_only=retry_no_transcript)
                    if channel_stats:
                        all_stats['total_processed'] += channel_stats.get('total_processed', 0)
                        all_stats['speakers_detected'] += channel_stats.get('speakers_detected', 0)
                        all_stats['unknown_speakers'] += channel_stats.get('unknown_speakers', 0)
                        all_stats['new_speakers'].update(channel_stats.get('new_speakers', set()))
                        all_stats['csv_files_processed'].extend(channel_stats.get('csv_files_processed', []))
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
                            all_stats['csv_files_processed'].extend(channel_stats.get('csv_files_processed', []))
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
            
            speakers_after_set = load_json_file(SPEAKERS_FILE)
            all_stats.update(compute_speaker_inventory_delta(speakers_before_set, speakers_after_set))
            all_stats['speakers_changed_to_unknown'] = 0
            write_speaker_detection_log(all_stats, operation_name=f"Partial Scrape Menu (Last {days_back} Days)")
            print(f"\n✅ Partial scrape ({days_back} days) complete.")
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
            
            speakers_before_set = load_json_file(SPEAKERS_FILE)
            all_stats = {'total_processed': 0, 'speakers_detected': 0, 'unknown_speakers': 0, 'by_church': {}, 'new_speakers': set(), 'csv_files_processed': []}
            
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
                        all_stats['csv_files_processed'].extend(channel_stats.get('csv_files_processed', []))
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
                            all_stats['csv_files_processed'].extend(channel_stats.get('csv_files_processed', []))
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
            
            speakers_after_set = load_json_file(SPEAKERS_FILE)
            all_stats.update(compute_speaker_inventory_delta(speakers_before_set, speakers_after_set))
            all_stats['speakers_changed_to_unknown'] = 0
            write_speaker_detection_log(all_stats, operation_name="Unscraped Channels Scrape")
            print(f"\n✅ Unscraped channels scrape complete.")
            return

        if action == '8':
            print("\n--- BACKFILL VIDEO DURATION (Metadata Scrape) ---")
            print("This scans videos for duration metadata and fetches it from YouTube.")
            print("It automatically updates transcript headers with standard time format (Hours:Minutes).")
            print("It also re-heals video types (e.g., Short Clip vs Sermon) based on the new duration.\n")
            
            print(" 1. Scan MISSING durations only (Faster)")
            print(" 2. Scan ALL videos (Update/Overwrite everything)")
            sub_choice = input("\n👉 Enter choice (1-2): ").strip()
            
            force_all_choice = False
            if sub_choice == '2':
                force_all_choice = True
            elif sub_choice != '1':
                print("Invalid choice, defaulting to MISSING only.")
            
            print("\nAvailable channels:")
            channel_names = list(channels.keys())
            for i, name in enumerate(channel_names, 1):
                print(f"  {i}. {name}")
            print(f"  0. All Channels")
            
            choice = input("\n👉 Enter channel number (or 0 for all): ").strip()
            
            churches_arg = None
            if choice != '0' and choice:
                try:
                    idx = int(choice) - 1
                    if 0 <= idx < len(channel_names):
                        churches_arg = channel_names[idx]
                        print(f"\n📂 Targeted Channel: {churches_arg}")
                    else:
                        print("Invalid selection.")
                        return
                except ValueError:
                    print("Invalid input.")
                    return
            else:
                print("\n📂 Targeted Channel: ALL")
            
            backfill_duration_metadata(DATA_DIR, churches=churches_arg, force_all=force_all_choice)
            return

        if action == '9':
            print("\n--- BACKFILL MISSING TIMESTAMPED TRANSCRIPTS ---")
            print("This scans for plain transcript files (.txt) without a corresponding")
            print("timestamped version (.timestamped.txt) and fetches them from YouTube.\n")
            
            print(" 1. Process ALL Channels")
            print(" 2. Select a specific Channel")
            
            sub_choice = input("\n👉 Enter choice (1-2): ").strip()
            
            churches_arg = None
            
            if sub_choice == '1':
                print("\n📂 Targeted Channel: ALL")
            elif sub_choice == '2':
                print("\nAvailable channels:")
                channel_names = list(channels.keys())
                for i, name in enumerate(channel_names, 1):
                    print(f"  {i}. {name}")
                
                choice = input("\n👉 Enter channel number: ").strip()
                try:
                    idx = int(choice) - 1
                    if 0 <= idx < len(channel_names):
                        churches_arg = [channel_names[idx]]
                        print(f"\n📂 Targeted Channel: {churches_arg[0]}")
                    else:
                        print("Invalid selection.")
                        return
                except ValueError:
                    print("Invalid input.")
                    return
            else:
                print("Invalid selection.")
                return
            
            backfill_timestamps(DATA_DIR, churches=churches_arg)
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
                    speakers_before_set = load_json_file(SPEAKERS_FILE)
                    channel_stats = process_channel(channel_name, channels[channel_name], known_speakers)
                    if channel_stats and channel_stats.get('total_processed', 0) > 0:
                        all_stats = {
                            'total_processed': channel_stats.get('total_processed', 0),
                            'speakers_detected': channel_stats.get('speakers_detected', 0),
                            'unknown_speakers': channel_stats.get('unknown_speakers', 0),
                            'new_speakers': channel_stats.get('new_speakers', set()),
                            'csv_files_processed': channel_stats.get('csv_files_processed', []),
                            'by_church': {channel_name: {
                                'total': channel_stats.get('total_processed', 0),
                                'detected': channel_stats.get('speakers_detected', 0),
                                'unknown': channel_stats.get('unknown_speakers', 0)
                            }}
                        }
                        speakers_after_set = load_json_file(SPEAKERS_FILE)
                        all_stats.update(compute_speaker_inventory_delta(speakers_before_set, speakers_after_set))
                        all_stats['speakers_changed_to_unknown'] = 0
                        write_speaker_detection_log(all_stats, operation_name=f"Single Channel Scrape: {channel_name}")
                else:
                    print("Invalid selection.")
            except ValueError:
                print("Invalid input.")
        elif action == '2':
            print("\n--- ALL CHANNELS SCRAPE ---")
            print(" 1. Force Scrape All Channels (Scan everything)")
            print(" 2. Scrape New Channels Only (Skip churches with existing data)")
            print(" 3. Scrape Channels with Missing Transcripts Only")
            sub_choice = input("\n👉 Enter Mode (1-3): ").strip()

            target_channels = channels.items()
            retry_no_transcript = False

            if sub_choice == '2':
                # Filter to unscraped only
                filtered = []
                for name, config in channels.items():
                    summary_path = get_summary_file_path(name, ".csv")
                    if not os.path.exists(summary_path):
                        filtered.append((name, config))
                target_channels = filtered
                if not target_channels:
                    print("✅ No new channels found.")
                    return
                print(f"📊 Found {len(target_channels)} new channels.")

            elif sub_choice == '3':
                retry_no_transcript = True
                # Start with all channels, process_channel will filter internally 
                # or we can pre-filter here if we want to be smarter, but process_channel handles it.
                pass 
            
            elif sub_choice != '1':
                print("Invalid selection.")
                return

            speakers_before_set = load_json_file(SPEAKERS_FILE)
            all_stats = {'total_processed': 0, 'speakers_detected': 0, 'unknown_speakers': 0, 'by_church': {}, 'new_speakers': set(), 'csv_files_processed': []}
            
            # Sort channels for consistent processing order
            # If standard/force scrape: unscraped first, then alphabetical
            def channel_sort_key(item):
                name, config = item
                summary_path = get_summary_file_path(name, ".csv")
                has_summary = os.path.exists(summary_path)
                return (0 if not has_summary else 1, name.lower())
            
            sorted_channels = sorted(target_channels, key=channel_sort_key)
            total_channels = len(sorted_channels)
            
            for idx, (name, config) in enumerate(sorted_channels, 1):
                remaining = total_channels - idx
                print(f"\n{'='*60}")
                print(f"📺 CHANNEL {idx}/{total_channels}: {name}")
                print(f"{'='*60}")
                
                channel_stats = process_channel(name, config, known_speakers, retry_no_transcript_only=retry_no_transcript)
                if channel_stats:
                    all_stats['total_processed'] += channel_stats.get('total_processed', 0)
                    all_stats['speakers_detected'] += channel_stats.get('speakers_detected', 0)
                    all_stats['unknown_speakers'] += channel_stats.get('unknown_speakers', 0)
                    all_stats['new_speakers'].update(channel_stats.get('new_speakers', set()))
                    all_stats['csv_files_processed'].extend(channel_stats.get('csv_files_processed', []))
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
                    
            speakers_after_set = load_json_file(SPEAKERS_FILE)
            all_stats.update(compute_speaker_inventory_delta(speakers_before_set, speakers_after_set))
            all_stats['speakers_changed_to_unknown'] = 0
            
            log_title = "All Channels Scrape"
            if sub_choice == '2': log_title = "New Channels Only Scrape"
            if sub_choice == '3': log_title = "Retry Missing Transcripts Scrape"
            
            write_speaker_detection_log(all_stats, operation_name=log_title)
            print(f"\n✅ {log_title} complete.")
            
            # Generate master CSV after all channels are processed
            print("\n📊 Generating master CSV...")
            generate_master_csv()
        else:
            print("Invalid action. Exiting.")
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        # Always generate master CSV at the end, regardless of which action was taken
        print("\n📊 Generating master CSV...")
        generate_master_csv()

# === Ensure this is at the very end of the file ===
print("=== update_sermons.py script started ===")
if __name__ == "__main__":
    main()