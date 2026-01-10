import json
import csv
import glob
import re
import os

SPEAKERS_FILE = 'speakers.json'
DATA_DIR = 'data'

def load_speakers():
    if not os.path.exists(SPEAKERS_FILE):
        print(f"Error: {SPEAKERS_FILE} not found.")
        return []
    with open(SPEAKERS_FILE, 'r') as f:
        return json.load(f)

def load_summary_titles():
    # Map of Title -> List of (Filename, SourceRow)
    titles = {}
    files = glob.glob(os.path.join(DATA_DIR, "*_Summary.csv"))
    print(f"Scanning {len(files)} summary files for titles...")
    
    for path in files:
        filename = os.path.basename(path)
        try:
            with open(path, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    t = row.get('title', '').strip()
                    if t:
                        if t not in titles:
                            titles[t] = []
                        titles[t].append(filename)
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            
    return titles

def is_suspicious_structure(name):
    reasons = []
    
    # Check for digits
    if re.search(r'\d', name):
        reasons.append("Contains Digits")
        
    # Check for parens or quotes
    if re.search(r'[()"\'\[\]]', name):
        reasons.append("Contains Parens/Quotes")
        
    # Check length
    words = name.split()
    if len(words) > 4:
        reasons.append("Too Long (>4 words)")
    if len(words) == 1 and name.lower() not in ["jesus", "god", "christ"]: # Single words are often suspect unless whitelisted
         # But many single names are valid (e.g. "Branham"). Be careful.
         # So we won't flag generic single words unless they are bad keywords.
         pass

    # Check for " and " without honorifics
    if " and " in name.lower() or " & " in name.lower():
        if not any(h in name.lower() for h in ["bro", "sis", "pastor", "rev", "bish", "elder", "dr"]):
            reasons.append("And without Honorific")

    # Check for bad keywords
    bad_keywords = [
        "service", "meeting", "worship", "sermon", "prayer", "tabernacle", "church", 
        "assembly", "fellowship", "ministries", "ministry", "choir", "band", "song",
        "title", "unknown", "speaker", "video", "clip", "audio", "part", "pt", "vol",
        "chapter", "verse", "bible", "scripture", "message", "testimony", "dedication",
        "communion", "baptism", "supper", "revival", "conference", "convention",
        "spiritual", "supernatural", "divine", "holy", "ghost", "spirit", "god", "jesus",
        "christ", "lord", "offering", "tithing", "school", "class", "lesson", "study",
        "wednesday", "sunday", "morning", "evening", "night", "live", "stream",
        "broadcast", "recording", "tape", "cd", "dvd", "mp3", "digital", "download",
        "mission", "report", "update", "news", "announcement", "welcome", "opening",
        "closing", "remarks", "intro", "introduction", "outro", "conclusion",
        "question", "answer", "discussion", "panel", "interview", "conversation",
        "visit", "visitor", "guest", "special", "item", "song", "singing", "music",
        "instrumental", "piano", "organ", "guitar", "violin", "trumpet", "saxophone",
        "flute", "drums", "bass", "solo", "duet", "trio", "quartet", "ensemble",
        "group", "family", "friends", "brethren", "saints", "believers", "youth",
        "children", "kids", "men", "women", "ladies", "brothers", "sisters",
        "fathers", "mothers", "parents", "couples", "seniors", "adults", "teens",
        "young", "old", "new", "ancient", "modern", "future", "past", "present",
        "history", "story", "life", "death", "birth", "marriage", "wedding", "funeral",
        "memorial", "tribute", "honor", "respect", "love", "hate", "war", "peace",
        "joy", "sorrow", "pain", "pleasure", "hope", "faith", "charity", "grace",
        "mercy", "truth", "justice", "righteousness", "holiness", "sin", "evil",
        "good", "bad", "right", "wrong", "up", "down", "in", "out", "on", "off",
        "over", "under", "above", "below", "before", "after", "during", "while",
        "when", "where", "why", "how", "what", "who", "which", "that", "this",
        "these", "those", "here", "there", "then", "now", "always", "never",
        "sometimes", "often", "seldom", "rarely", "usually", "normally", "typically",
        "generally", "mostly", "mainly", "chiefly", "primarily", "essentially",
        "fundamentally", "basically", "literally", "virtually", "practically",
        "actually", "really", "truly", "verily", "surely", "certainly", "definitely",
        "absolutely", "positively", "exactly", "precisely", "specifically",
        "particularly", "especially", "notably", "significantly", "importantly",
        "interestingly", "curiously", "surprisingly", "amazingly", "astonishingly",
        "remarkably", "incredibly", "unbelievably", "impossibly", "miraculously",
        "supernaturally", "divinely", "providentially", "accidentally",
        "coincidentally", "randomly", "arbitrarily", "haphazardly", "chaotically",
        "access", "authority", "dominion", "power", "glory", "kingdom",
        "throne", "crown", "scepter", "sword", "shield", "armor", "helmet",
        "breastplate", "shoes", "girdle", "robe", "garment", "clothing",
        "apparel", "attire", "dress", "outfit", "costume", "uniform", "vesture",
        "raiment", "covering", "mantle", "cloak", "cape", "coat", "jacket",
        "vest", "shirt", "pants", "trousers", "skirt", "blouse", "shoes",
        "boots", "sandals", "slippers", "socks", "stockings", "gloves", "mittens",
        "hat", "cap", "bonnet", "hood", "veil", "scarf", "shawl", "belt",
        "sash", "girdle", "apron", "bib", "collar", "cuff", "sleeve", "pocket",
        "button", "zipper", "buckle", "lace", "tie", "bow", "ribbon", "string",
        "thread", "yarn", "fabric", "cloth", "material", "access", "resurrection",
        "rapture", "coming", "prophecy", "prophetic", "vision", "dream",
        "revelation", "inspiration", "illumination", "enlightenment", "understanding",
        "knowledge", "wisdom", "intelligence", "mind", "thought", "idea",
        "concept", "principle", "doctrine", "teaching", "preaching", "gospel",
        "evangelism", "witnessing", "testifying", "confessing", "believing",
        "repenting", "forgiving", "redeeming", "saving", "delivering", "healing",
        "restoring", "blessing", "cursing", "binding", "loosing", "casting",
        "driving", "walking", "running", "jumping", "dancing", "shouting",
        "singing", "praising", "worshipping", "serving", "ministering", "giving",
        "receiving", "taking", "holding", "keeping", "losing", "finding",
        "seeking", "searching", "looking", "watching", "waiting", "hoping",
        "trusting", "resting", "sleeping", "waking", "rising", "standing",
        "sitting", "kneeling", "bowing", "falling", "lying", "dying", "living",
        "breathing", "eating", "drinking", "tasting", "smelling", "hearing",
        "seeing", "feeling", "touching", "sensing", "perceiving", "knowing",
        "understanding", "comprehending", "realizing", "recognizing",
        "remembering", "forgetting", "thinking", "imagining", "dreaming",
        "planning", "choosing", "deciding", "intending", "willing", "desiring",
        "wishing", "hoping", "fearing", "dreading", "hating", "loving",
        "liking", "disliking", "preferring", "rejecting", "refusing",
        "accepting", "admiring", "adoring", "worshipping", "honoring",
        "respecting", "disrespecting", "mocking", "scorning", "despising",
        "pitying", "envying", "jealous", "pride", "humility", "boldness",
        "courage", "fear", "timidity", "shyness", "modesty", "shame",
        "guilt", "innocence", "justice", "injustice", "fairness", "unfairness"
    ]
    
    name_lower = name.lower()
    for kw in bad_keywords:
        # Check strict word boundary for short keywords to avoid false positives
        if len(kw) <= 3:
            if re.search(r'\b' + re.escape(kw) + r'\b', name_lower):
                 reasons.append(f"Contains keyword '{kw}'")
        else:
            if kw in name_lower:
                reasons.append(f"Contains keyword '{kw}'")

    return reasons

def deep_audit():
    speakers = load_speakers()
    titles = load_summary_titles()
    
    print(f"Auditing {len(speakers)} speakers...")
    
    flagged = []
    
    for s in speakers:
        reasons = is_suspicious_structure(s)
        
        # Check title matches
        s_clean = s.strip()
        if s_clean in titles:
            reasons.append(f"Matches Title Exactly ({len(titles[s_clean])} times)")
        
        # Check partial title matches for long names
        # Logic: If a speaker name is Long and is found INSIDE a title, unlikely to be a name
        if len(s_clean.split()) > 3:
            # This is expensive, so maybe skip or optimize.
            # Let's skip deep partial matching for speed, exact match is powerful enough usually.
            pass
            
        if reasons:
            # Filter slight noise
            # If ONLY keyword match and keyword is generic like "Don", skip (handled by boundary check)
            flagged.append({
                "speaker": s,
                "reasons": "; ".join(reasons)
            })
            
    # Sort by number of reasons
    flagged.sort(key=lambda x: len(x['reasons']), reverse=True)
    
    # Save results
    out_file = "deep_audit_results.csv"
    with open(out_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["speaker", "reasons"])
        writer.writeheader()
        writer.writerows(flagged)
        
    print(f"Deep Audit Complete. Found {len(flagged)} suspicious speakers.")
    print(f"Results saved to {out_file}")
    
    # Preview top 20
    print("\n--- Top Suspicious Speakers ---")
    for row in flagged[:50]:
        print(f"[{row['speaker']}] -> {row['reasons']}")

if __name__ == "__main__":
    deep_audit()
