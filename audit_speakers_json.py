import csv
import json
import re
from collections import Counter
from pathlib import Path

SPEAKERS_JSON = Path('speakers.json')
OUT_CSV = Path('speakers_json_erroneous_candidates.csv')

STOPWORDS = {
    'the','a','an','and','or','but','of','in','on','at','for','with','without','from','by','to','into','over','under','vs','pt','part'
}
# Some surname particles that are valid in names
NAME_PARTICLES = {'de','del','de la','la','van','von','da','di','du','st','st.'}

HONORIFICS_PREFIX = (
    'bro ', 'bro. ', 'sis ', 'sis. ', 'past ', 'past. ', 'pastor ', 'pstr ', 'min ', 'min. ', 'dr ', 'dr. ', 'apostle '
)
HONORIFICS_SUFFIX = {
    'bro','bro.','sis','sis.','past','past.','pastor','pstr','min','min.','dr','dr.','apostle'
}

TITLE_KEYWORDS = {
    'service','meeting','prayer','worship','communion','convention','conference','camp','youth','banquet','wedding',
    'funeral','memorial','tribute','report','broadcast','livestream','live','school','seminar','retreat','concert',
    'anniversary','graduation','christmas','easter','watch','night',
    'pt','part','series','episode','review'
}

re_dateish = re.compile(r'(\b\d{4}-\d{2}-\d{2}\b|\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b)')


def normalize_spaces(s: str) -> str:
    return re.sub(r'\s+', ' ', s or '').strip()


def tokens(s: str) -> list[str]:
    return [t for t in normalize_spaces(s).split(' ') if t]


def looks_person_like(name: str) -> bool:
    """Loose check: 2-3 tokens, not obviously a title."""
    t = tokens(name)
    if len(t) < 2 or len(t) > 3:
        return False
    low = name.lower()
    if re_dateish.search(name):
        return False
    if any(k in low for k in TITLE_KEYWORDS):
        return False
    # allow particles in the middle like "de", "van"
    # require at least two alphabetic-starting tokens
    alpha_tokens = [x for x in t if re.match(r'^[A-Za-zÀ-ÖØ-öø-ÿ]', x)]
    return len(alpha_tokens) >= 2


def detect_reasons(name: str, all_names_set: set[str], base_person_names: set[str]) -> list[str]:
    reasons: list[str] = []
    s = normalize_spaces(name)
    low = s.lower()
    t = tokens(s)

    if not s:
        return ['empty']

    if s.endswith('.timestamped') or '.timestamped' in s:
        reasons.append('contains_timestamped')

    if low.startswith(HONORIFICS_PREFIX):
        reasons.append('honorific_prefix')

    if t and t[-1].lower().strip('.,:;!-') in HONORIFICS_SUFFIX:
        reasons.append('honorific_suffix')

    if re_dateish.search(s):
        reasons.append('has_date_or_digits')

    if any(ch in s for ch in ['|', '…', '...']):
        reasons.append('has_delimiters')

    # stopwords inside a supposed name (excluding known particles)
    joined_low = ' '.join(t).lower()
    if joined_low not in NAME_PARTICLES:
        for w in t:
            wlow = w.lower().strip('.,:;!()[]{}')
            if wlow in STOPWORDS and wlow not in {'de', 'la', 'van', 'von', 'da', 'di', 'du'}:
                reasons.append('contains_stopword')
                break

    # title keywords anywhere
    if any(k in low for k in TITLE_KEYWORDS):
        reasons.append('contains_title_keyword')

    # Prefix contamination: name starts with a shorter person-like name in the list
    # Example: "Chad Lamb Access" when "Chad Lamb" exists.
    for base in base_person_names:
        if s != base and s.startswith(base + ' '):
            reasons.append('prefix_contamination')
            break

    return reasons


def main() -> None:
    speakers = json.loads(SPEAKERS_JSON.read_text(encoding='utf-8'))
    speakers = [normalize_spaces(s) for s in speakers]

    all_names = set(speakers)
    base_person_names = {s for s in all_names if looks_person_like(s)}

    rows = []
    reason_counts = Counter()

    for s in speakers:
        reasons = detect_reasons(s, all_names, base_person_names)
        if reasons:
            # Filter out "Unknown Speaker" only — user asked to detect erroneous speakers; this isn't in speakers.json typically anyway
            if s == 'Unknown Speaker':
                continue
            for r in reasons:
                reason_counts[r] += 1
            rows.append({'speaker': s, 'reasons': ';'.join(sorted(set(reasons)))})

    # Reduce false positives: keep only entries with at least one strong signal.
    strong = {'contains_timestamped', 'has_date_or_digits', 'contains_title_keyword', 'contains_stopword', 'prefix_contamination', 'honorific_prefix', 'honorific_suffix', 'has_delimiters'}
    filtered = [r for r in rows if any(x in strong for x in r['reasons'].split(';'))]

    with OUT_CSV.open('w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['speaker', 'reasons'])
        w.writeheader()
        w.writerows(filtered)

    print(f"Total entries in speakers.json: {len(speakers)}")
    print(f"Base person-like names (2-3 tokens): {len(base_person_names)}")
    print(f"Erroneous-candidate entries written: {len(filtered)}")
    print(f"Report: {OUT_CSV}")

    print('Top reason counts:')
    for reason, count in reason_counts.most_common(10):
        print(f"  {reason}: {count}")


if __name__ == '__main__':
    main()
